
from azure.storage.blob import BlobServiceClient
import fitz  # PyMuPDF
import os, json, requests, time, functools, base64, sys, logging
from io import BytesIO
from openai import AzureOpenAI



# 環境変数: Blob Storage
BLOB_STORAGE_CONNECTION_STRING = os.getenv('BlobStorage_ConnectionString')
BLOB_STORAGE_CONTAINER_NAME  = os.getenv('BlobStorage_ContainerName')
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)
last_file_list = os.getenv('BlobStorage_last_file_list')


# 環境変数: AI Search
AI_SEARCH_SERVICE_URL = os.getenv('AI_SEARCH_SERVICE_URL')
AI_SEARCH_API_KEY = os.getenv('AI_SEARCH_API_KEY')
AI_SEARCH_API_VERSION = os.getenv('AI_SEARCH_API_VERSION')
AI_SEARCH_INDEX_NAME  = os.getenv('AI_SEARCH_INDEX_NAME')


# 環境変数: AOAI
AOAI_EMBEDDING_MODEL = os.getenv('AOAI_EMBEDDING_MODEL')
AOAI_ENDPOINT = os.getenv("AOAI_ENDPOINT")
AOAI_API_KEY = os.getenv("AOAI_API_KEY")
AOAI_API_VERSION = os.getenv("AOAI_API_VERSION")
client = AzureOpenAI(
    api_key = AOAI_API_KEY,  
    azure_endpoint = AOAI_ENDPOINT,
    api_version = AOAI_API_VERSION
)


# 429 too many request 回避用にリトライデコレータを作成する
# [todo]リトライを徐々に遅らせる事でfunctionのタイムアウト制限（デフォルト5分）に引っかかる可能性が高まる
def exponential_backoff(retries=5, backoff_in_seconds=2, max_backoff_in_seconds=64):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    logging.info(f"Attempt {attempt + 1} of {retries}...")
                    response = func(*args, **kwargs)
                    return response
                except Exception as e:  # Modify this line
                    wait = min(max_backoff_in_seconds, backoff_in_seconds * (2 ** attempt))
                    logging.info(f"Request failed with {e}, retrying in {wait} seconds...")
                    time.sleep(wait)
                    attempt += 1
        return wrapper_retry
    return decorator_retry


# ベクトル化は429が多発するのでリトライデコレータを付与する
@exponential_backoff()
def get_embedding(text):
    response = client.embeddings.create(
        input=text, 
        model=AOAI_EMBEDDING_MODEL
    )
    return response.data[0].embedding



def extract_text_from_pdf(blob):
    blob_client = BLOB_SERVICE_CLIENT.get_blob_client(BLOB_STORAGE_CONTAINER_NAME, blob)
    pdf_data = blob_client.download_blob().readall()
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    text = ""

    try:
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            text += page.get_text()
        
    except Exception as e:
        logging.error(f"Error extracting text: {str(e)}")
        text = "error"
    return text


def encode_document_key(key):
    return base64.urlsafe_b64encode(key.encode()).decode()



##################################################################
# index更新対象のファイルを取得する
##################################################################
def get_updated_files():
    container_client = BLOB_SERVICE_CLIENT.get_container_client(BLOB_STORAGE_CONTAINER_NAME)
    current_file_list = [blob.name for blob in container_client.list_blobs()]
    last_file_list = load_last_file_list()

    # last_file_list.json自身を除外する
    current_file_list = [f for f in current_file_list if f != last_file_list]

    new_files = list(set(current_file_list) - set(last_file_list))
    deleted_files = list(set(last_file_list) - set(current_file_list))
    updated_files = list(set(current_file_list) & set(last_file_list)) 
    
    save_current_file_list(current_file_list)
    
    return new_files, deleted_files, updated_files


def save_current_file_list(file_list):
    blob_client = BLOB_SERVICE_CLIENT.get_blob_client(BLOB_STORAGE_CONTAINER_NAME, last_file_list)
    blob_data = json.dumps(file_list, ensure_ascii=False).encode('utf-8')
    blob_client.upload_blob(BytesIO(blob_data), overwrite=True)


def load_last_file_list():
    try:
        blob_client = BLOB_SERVICE_CLIENT.get_blob_client(BLOB_STORAGE_CONTAINER_NAME, last_file_list)
        blob_data = blob_client.download_blob().readall().decode('utf-8')
        return json.loads(blob_data)
    except Exception as e:
        logging.info(f"Failed to load last file list: {str(e)}")
        return []


##################################################################
# indexを更新する
##################################################################
def update_search_index(documents):
    url = f"{AI_SEARCH_SERVICE_URL}/indexes/{AI_SEARCH_INDEX_NAME}/docs/index?api-version={AI_SEARCH_API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": AI_SEARCH_API_KEY
    }
    data = {
        "value": documents
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error: {e.response.text}")
        raise
