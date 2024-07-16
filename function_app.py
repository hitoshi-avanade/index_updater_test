import azure.functions as func
import logging, os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# 日本のTZ=UTC+9時間なので、UTC 15:00実行=日本 0:00実行
@app.timer_trigger(schedule=os.getenv('TIMER_SCHEDULE'), arg_name="mytimer", run_on_startup=False, use_monitor=True)
def index_updater(mytimer: func.TimerRequest) -> None:

    # 以下のimportは関数内で行わないと、azure上で関数定義が見えなくなる現象が発生している
    from func_module import get_updated_files, encode_document_key, extract_text_from_pdf, get_embedding, update_search_index
    import time, sys

    sys.stdout.reconfigure(encoding='utf-8')
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # index更新対象のファイルを取得する
        # [todo]必要に応じてロジック変更必要かも
        new_files, deleted_files, updated_files = get_updated_files()
        logging.info(f"New files: {new_files}")
        logging.info(f"Deleted files: {deleted_files}")
        logging.info(f"Updated files: {updated_files}")

        # index更新対象ファイルのindex更新用データを一括作成する
        # [todo] 一括更新で遅延する場合は、1ファイルごとに更新するよう変更が必要になる可能性があります
        documents = []
        for filename in new_files + updated_files:
            text = extract_text_from_pdf(filename)

            # 新規登録/更新ファイルの登録データの作成
            document = {
                "@search.action": "mergeOrUpload",
                "id": encode_document_key(filename),
                "filename": filename,
                "embedding": get_embedding(text),
                "content": text
            }
            documents.append(document)

            # embeddingで429エラーが発生しないように少し待機する
            time.sleep(1)

        # 削除ファイルの登録データの作成
        for filename in deleted_files:
            document = {
                "@search.action": "delete",
                "id": encode_document_key(filename),
            }
            documents.append(document)
        
        
        # ファイルの更新があればindexを一括更新する
        if documents:
            update_search_index(documents)
        

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        #return func.HttpResponse(f"Error: {str(e)}", status_code=500)
