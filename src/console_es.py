from flask import Flask, request, jsonify
import json
import requests
import pandas as pd
from omegaconf import OmegaConf
import logging
import os

from individual_file_local import IndividualFileLocal



app = Flask(__name__)

# 설정 파일 로드
cfg = OmegaConf.load(str(r'../cfg/local_cfg.yaml'))

# Elasticsearch 설정 가져오기
es_url = cfg.es.url
index_name = cfg.es.index_name
auth = (cfg.es.user, cfg.es.pw)
upload_path = cfg.local.path

app.config['UPLOAD_FOLDER'] = upload_path
ALLOWED_EXTENSIONS = {"pdf", "xls", "xlsx", "txt", "xlsx", "xlrd"}



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

# 2. Sub 폴더 리스트 조회
@app.route('/get_sub_folders', methods=['GET'])
def get_sub_folders():
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
    response = requests.get(f"{es_url}/{index_name}/_search", headers={"Content-Type": "application/json"}, data=json.dumps(query), auth=auth)
    return jsonify(response.json())

# 3. 특정 폴더 내의 파일 리스트 조회 body : folder_name
@app.route('/get_files_in_folder', methods=['GET'])
def get_files_in_folder():
    folder_name = request.json.get('folder_name')
    query = {
        "query": {
            "term": {
                "metadata.group.keyword": folder_name
            }
        },
        "_source": ["metadata.source", "metadata.group"]
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


# 7. 특정 folder_name에 파일들을 저장하는 API
@app.route('/upload_files', methods=['POST'])
def upload_files():
    if 'folder_name' not in request.form:
        return jsonify({"error": "folder_name is missing"}), 400

    folder_name = request.form.get('folder_name')
    if not folder_name:
        return jsonify({"error": "folder_name is missing"}), 400
    folder_path = os.path.join(app.config['UPLOAD_FOLDER'], folder_name)

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



# 8. 특정 folder_name에 파일들을 embedding하는 API
@app.route('/generate_embedding', methods=['POST'])
def generate_embedding_api():
    data = request.json
    folder_name = data.get('folder_name')
    if not folder_name:
        return jsonify({"error": "No folder_name provided"}), 400

    try:
        embedding = IndividualFileLocal(folder_name)
        return jsonify({"embedding": embedding.tolist()}) 
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
