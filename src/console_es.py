from flask import Flask, request, jsonify
import json
import psycopg2
import requests
import pandas as pd
from omegaconf import OmegaConf
import logging
import os
import time

from individual_file_local import IndividualFileLocal
from extractor import Extractor
from loader import Loader
import queue
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# embedding_queue = queue.Queue()
small_file_queue = queue.Queue()
large_file_queue = queue.Queue()


app = Flask(__name__)

# 설정 파일 로드
# cfg = OmegaConf.load(str(r'/workplace/cfg/local_cfg.yaml'))
config_path = os.getenv('CONFIG_PATH', '/workplace/cfg/local_cfg.yaml')
cfg = OmegaConf.load(config_path) 

# Elasticsearch 설정 가져오기
es_url = cfg.es.url
index_name = cfg.es.index_name
auth = (cfg.es.user, cfg.es.pw)
upload_path = cfg.local.path

app.config['UPLOAD_FOLDER'] = upload_path
ALLOWED_EXTENSIONS = {"pdf", "xls", "xlsx", "txt", "xlsx", "xlrd"}

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

app.logger.setLevel(logging.INFO) 

logging.info("Flask app is starting up...")

# TEST db연결로 문제가 생겼는지 확인하기 위해
def test_file_access(path):
    path = os.path.normpath(path)  # Normalize the path
    logging.info(f"Testing file access for: {path}")
    
    # Check if path exists
    exists = os.path.exists(path)
    logging.info(f"Path exists: {exists}")
    
    if exists:
        if os.path.isdir(path):
            logging.info("Path is a directory.")
            if os.access(path, os.R_OK):
                logging.info("Read access is available.")
            else:
                logging.error("Read access is not available.")
        elif os.path.isfile(path):
            logging.info("Path is a file.")
            if os.access(path, os.R_OK):
                logging.info("Read access to the file is available.")
            else:
                logging.error("Read access to the file is not available.")
        else:
            logging.error("Path exists but is neither a file nor a directory.")
    else:
        logging.error("Path does not exist.")

def test_db_connection():
    try:
        db_conn = psycopg2.connect(
        host=cfg.database.host,
        port=cfg.database.port,
        dbname=cfg.database.db_name,
        user=cfg.database.user,
        password=cfg.database.pw
        )
        db_cursor = db_conn.cursor()
        # conn = psycopg2.connect("dbname=test user=postgres password=secret")
        logging.info("Database connection successful.")
        db_cursor.close()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")



# Q-1. 큐 상태 확인 API
@app.route('/queue_status', methods=['GET'])
def queue_status():
    small_queue_size = small_file_queue.qsize()
    large_queue_size = large_file_queue.qsize()
    return jsonify({
        "small_queue_size": small_queue_size,
        "large_queue_size": large_queue_size
    }), 200

# # Q-2. 큐 Clear API
# @app.route('/clear_queue', methods=['POST'])
# def clear_queue():
#     while not embedding_queue.empty():
#         embedding_queue.get()
#     return jsonify({"message": "Queue cleared."}), 200

# Q-2. 큐 Clear API
@app.route('/clear_queue', methods=['POST'])
def clear_queue():
    queue_type = request.args.get('type')

    if queue_type == 'small':
        while not small_file_queue.empty():
            small_file_queue.get()
        return jsonify({"message": "Small queue cleared."}), 200

    elif queue_type == 'large':
        while not large_file_queue.empty():
            large_file_queue.get()
        return jsonify({"message": "Large queue cleared."}), 200

    return jsonify({"error": "Invalid queue type. Please specify 'small' or 'large'."}), 400




# 1. 전체 파일 리스트 조회 최대 10000개
@app.route('/get_all_files', methods=['GET'])
def get_all_files():
    query = {
        "size": 0,
        "aggs": {
            "distinct_files": {
                "terms": {
                    "field": "metadata.source.keyword",
                    "size": 10000
                }
            }
        }
    }
    response = requests.get(f"{es_url}/{index_name}/_search", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())

# 2. Sub 폴더 리스트 조회 10000개
@app.route('/get_sub_folders', methods=['GET'])
def get_sub_folders():
    test_path = "/home/minds/amore-embedding-data"
    # file_access_message = test_file_access(test_path)
    # db_connection_message = test_db_connection()
    query = {
        "size": 0,
        "aggs": {
            "distinct_files": {
                "terms": {
                    "field": "metadata.group.keyword",
                    "size": 10000
                }
            }
        }
    }
    # logging.info(file_access_message)
    # logging.info(db_connection_message)
    response = requests.get(f"{es_url}/{index_name}/_search", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())

# 3. 특정 폴더 내의 파일 리스트 조회 body : folder_name 10000개
@app.route('/get_files_in_folder', methods=['GET'])
def get_files_in_folder():
    folder_name = request.json.get('folder_name')
    query = {
        "size": 0,  
        "query": {
            "term": {
                "metadata.group.keyword": folder_name
            }
        },
        "aggs": {
            "distinct_files": {
                "terms": {
                    "field": "metadata.source.keyword",
                    "size": 10000 
                }
            }
        }
    }
    response = requests.get(f"{es_url}/{index_name}/_search", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())

# 4. 특정 폴더 지우기 body : folder_name
@app.route('/delete_folder', methods=['POST'])
def delete_folder():
    folder_name = request.json.get('folder_name')
    query = {
        "query": {
            "term": {
                "metadata.group.keyword": folder_name
            }
        }
    }
    response = requests.post(f"{es_url}/{index_name}/_delete_by_query", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())

# 5. 특정 파일 지우기 body : file_name
@app.route('/delete_file', methods=['POST'])
def delete_file():
    file_name = request.json.get('file_name')
    query = {
        "query": {
            "term": {
                "metadata.source.keyword": file_name
            }
        }
    }

    response = requests.post(f"{es_url}/{index_name}/_delete_by_query", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())

# 6. 특정 폴더 내의 특정 파일 지우기 body : folder_name, file_name
@app.route('/delete_file_in_folder', methods=['POST'])
def delete_file_in_folder():
    data = request.json
    folder_name = data.get('folder_name')
    file_name = data.get('file_name')
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "metadata.group.keyword": folder_name
                        }
                    },
                    {
                        "term": {
                            "metadata.source.keyword": file_name
                        }
                    }
                ]
            }
        }
    }
    response = requests.post(f"{es_url}/{index_name}/_delete_by_query", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# 7. 특정 folder_name에 파일들을 저장하는 API form-data body : folder_name(key), files(key)
@app.route('/upload_files', methods=['POST'])
def upload_files():
    if 'folder_name' not in request.form:
        return jsonify({"error": "folder_name is missing"}), 400

    folder_name = request.form.get('folder_name')
    if not folder_name:
        return jsonify({"error": "folder_name is missing"}), 400
    folder_path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)
    logging.info(f'*****************path***************** {folder_path}')

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    files = request.files.getlist('files')
    if not files:
        return jsonify({"error": "No files provided"}), 400

    saved_files = []
    for file in files:
        print(f"Original filename: {file.filename}")
        if file and allowed_file(file.filename):
            file_path = os.path.join(folder_path, file.filename)
            file.save(file_path)
            saved_files.append(file.filename)
        else:
            return jsonify({"error": f"File {file.filename} is not allowed"}), 400

    return jsonify({"message": f"Files saved in folder {folder_name}", "files": saved_files})



# queue에서 10개의 파일을 가져와서 처리
# def process_files_in_batch():
#     while True:
#         grouped_files = defaultdict(lambda: {"pdf": [], "xls": [], "xlsx": [], "txt": []})
#         batch_count = 0

#         # 큐에서 최대 10개의 파일을 꺼내서 chatbot_id와 folder_name 별로 그룹화
#         while not embedding_queue.empty() and batch_count < 10:
#             item = embedding_queue.get()
#             chatbot_id = item['chatbot_id']
#             folder_name = item['folder_name']
#             file_path = item['file']

#             if file_path.endswith('.pdf'):
#                 grouped_files[(chatbot_id, folder_name)]["pdf"].append(file_path)
#             elif file_path.endswith('.xls'):
#                 grouped_files[(chatbot_id, folder_name)]["xls"].append(file_path)
#             elif file_path.endswith('.xlsx'):
#                 grouped_files[(chatbot_id, folder_name)]["xlsx"].append(file_path)
#             elif file_path.endswith('.txt'):
#                 grouped_files[(chatbot_id, folder_name)]["txt"].append(file_path)

#             batch_count += 1

#         # 그룹화된 파일을 처리
#         for (chatbot_id, folder_name), files in grouped_files.items():
#             total_num = sum(len(file_list) for file_list in files.values())
#             if total_num > 0:
#                 try:
#                     logging.info(f"Processing batch of {total_num} files for chatbot_id: {chatbot_id}, folder_name: {folder_name}")
#                     IndividualFileLocal(chatbot_id, folder_name, files)
#                     logging.info(f"Successfully processed batch of {total_num} files.")
#                 except Exception as e:
#                     logging.error(f"Error processing files for chatbot_id: {chatbot_id}, folder_name: {folder_name}, error: {str(e)}")

#             # 처리한 후 그룹에서 해당 파일들 삭제
#             grouped_files[(chatbot_id, folder_name)] = {"pdf": [], "xls": [], "xlsx": [], "txt": []}

#         # 큐가 비어있으면 잠시 대기
#         if embedding_queue.empty():
#             time.sleep(2)

# executor = ThreadPoolExecutor(max_workers=1)
# executor.submit(process_files_in_batch)
# ThreadPoolExecutor 생성



# 작은 파일 큐에서 파일 처리
def process_small_files_in_batch():
    while True:
        grouped_files = defaultdict(lambda: {"pdf": [], "xls": [], "xlsx": [], "txt": []})
        batch_count = 0

        # 작은 파일 큐에서 최대 10개의 파일을 꺼내서 그룹화
        while not small_file_queue.empty() and batch_count < 10:
            item = small_file_queue.get()
            chatbot_id = item['chatbot_id']
            folder_name = item['folder_name']
            file_path = item['file']

            if file_path.endswith('.pdf'):
                grouped_files[(chatbot_id, folder_name)]["pdf"].append(file_path)
            elif file_path.endswith('.xls'):
                grouped_files[(chatbot_id, folder_name)]["xls"].append(file_path)
            elif file_path.endswith('.xlsx'):
                grouped_files[(chatbot_id, folder_name)]["xlsx"].append(file_path)
            elif file_path.endswith('.txt'):
                grouped_files[(chatbot_id, folder_name)]["txt"].append(file_path)

            batch_count += 1

        # 그룹화된 파일을 처리
        for (chatbot_id, folder_name), files in grouped_files.items():
            total_num = sum(len(file_list) for file_list in files.values())
            if total_num > 0:
                try:
                    logging.info(f"Processing small batch of {total_num} files for chatbot_id: {chatbot_id}, folder_name: {folder_name}")
                    IndividualFileLocal(chatbot_id, folder_name, files)
                    logging.info(f"Successfully processed small batch of {total_num} files.")
                except Exception as e:
                    logging.error(f"Error processing small files for chatbot_id: {chatbot_id}, folder_name: {folder_name}, error: {str(e)}")

        # 큐가 비어있으면 잠시 대기
        if small_file_queue.empty():
            time.sleep(2)

# 큰 파일 큐에서 파일 처리
def process_large_files_in_batch():
    while True:
        grouped_files = defaultdict(lambda: {"pdf": [], "xls": [], "xlsx": [], "txt": []})
        batch_count = 0

        # 큰 파일 큐에서 최대 10개의 파일을 꺼내서 그룹화
        while not large_file_queue.empty() and batch_count < 10:
            item = large_file_queue.get()
            chatbot_id = item['chatbot_id']
            folder_name = item['folder_name']
            file_path = item['file']

            if file_path.endswith('.pdf'):
                grouped_files[(chatbot_id, folder_name)]["pdf"].append(file_path)
            elif file_path.endswith('.xls'):
                grouped_files[(chatbot_id, folder_name)]["xls"].append(file_path)
            elif file_path.endswith('.xlsx'):
                grouped_files[(chatbot_id, folder_name)]["xlsx"].append(file_path)
            elif file_path.endswith('.txt'):
                grouped_files[(chatbot_id, folder_name)]["txt"].append(file_path)

            batch_count += 1

        # 그룹화된 파일을 처리
        for (chatbot_id, folder_name), files in grouped_files.items():
            total_num = sum(len(file_list) for file_list in files.values())
            if total_num > 0:
                try:
                    logging.info(f"Processing large batch of {total_num} files for chatbot_id: {chatbot_id}, folder_name: {folder_name}")
                    IndividualFileLocal(chatbot_id, folder_name, files)
                    logging.info(f"Successfully processed large batch of {total_num} files.")
                except Exception as e:
                    logging.error(f"Error processing large files for chatbot_id: {chatbot_id}, folder_name: {folder_name}, error: {str(e)}")

        # 큐가 비어있으면 잠시 대기
        if large_file_queue.empty():
            time.sleep(2)

executor = ThreadPoolExecutor(max_workers=2)

executor.submit(process_small_files_in_batch)
executor.submit(process_large_files_in_batch)



# 8. 특정 folder_name에 파일들을 embedding하는 API body : folder_name
@app.route('/generate_embedding', methods=['POST'])
def generate_embedding_api():
    data = request.json
    folder_name = data.get('folder_name')
    chatbot_id = data.get('chatbot_id')
    if not folder_name or not folder_name.strip():
        return jsonify({"error": "No valid folder_name provided"}), 400
    if not chatbot_id or not chatbot_id.strip():
        return jsonify({"error": "No valid chatbot_id provided"}), 400
    extractor = Extractor(cfg)
    loader = Loader(cfg)
    function_name = f"{chatbot_id}_{folder_name}"
    loader.delete_data_in_group(function_name)

    # 파일 리스트 가져오기
    files = extractor.get_file_list_from_local(
        local_path=os.path.join(cfg.local.path, f"{chatbot_id}_{folder_name}"), 
        prefix=cfg.local.prefix,
        file_extensions=["pdf", "xls", "xlsx", "txt"]
    )

    # 큐에 파일 추가 (각 파일에 대해 chatbot_id와 folder_name 포함)
    for file_type, file_list in files.items():
        logging.info(f"{len(file_list)} files of type {file_type} in {folder_name}.")
        for file in file_list:
            file_size = os.path.getsize(file)
            logging.info(f"파일 크기는 {file_size}.")
            if file_size > 10 * 1024 * 1024: 
                large_file_queue.put({"chatbot_id": chatbot_id, "folder_name": folder_name, "file": file})
            else:
                small_file_queue.put({"chatbot_id": chatbot_id, "folder_name": folder_name, "file": file})
    
            # embedding_queue.put({"chatbot_id": chatbot_id, "folder_name": folder_name, "file": file})
    
    return jsonify({"status": "added to queue"}), 200




# # 8. 특정 folder_name에 파일들을 embedding하는 API body : folder_name
# @app.route('/generate_embedding', methods=['POST'])
# def generate_embedding_api():
#     data = request.json
#     folder_name = data.get('folder_name')
#     chatbot_id = data.get('chatbot_id')
#     if not folder_name or not folder_name.strip():
#         return jsonify({"error": "No valid folder_name provided"}), 400
#     if not chatbot_id or not chatbot_id.strip():
#         return jsonify({"error": "No valid chatbot_id provided"}), 400

#     try:

#         results = IndividualFileLocal(chatbot_id, folder_name)
#         if results["errors"]:
#             return jsonify({"status": "completed with errors", "results": results}), 400

#         return jsonify({"status": "success", "results": results})

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500



if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=5001)
    port = int(os.getenv('PORT', 5001)) 
    app.run(host='0.0.0.0', port=port)
