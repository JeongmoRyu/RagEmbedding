import logging
import time
import os
from pathlib import Path
import uuid
from langchain.document_loaders import S3FileLoader, TextLoader, PyPDFLoader
from omegaconf import OmegaConf
from embedding.maum_embedding import MaumEmebedding
from extractor import Extractor
from transformer import Transformer
from loader import Loader
import pandas as pd
# from langchain.document_loaders import S3FileLoader
from pathlib import Path
from langchain.schema import Document
# import sys
import psycopg2
import threading
import queue

from embedding import create_embedding


# def read_excel_file(file_path):
#     """Read an Excel file into a DataFrame."""
#     try:
#         if file_path.endswith('.xlsx'):
#             df = pd.read_excel(file_path, engine='openpyxl')  # For .xlsx files
#         elif file_path.endswith('.xls'):
#             df = pd.read_excel(file_path, engine='xlrd')  # For .xls files
#         else:
#             raise ValueError(f"Unsupported file format for {file_path}")
#         return df
#     except Exception as e:
#         logging.error(f"Error reading {file_path}: {e}")
#         return None




def read_text_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def preprocessing(text: str):
    return text.replace("~", "-").replace("\n", "")




def ensure_db_connection(db_conn, db_cursor, cfg):
    try:
        if db_conn.closed != 0:
            logging.info("Reconnecting to the database...")
            db_conn = psycopg2.connect(
                host=cfg.database.host,
                port=cfg.database.port,
                dbname=cfg.database.db_name,
                user=cfg.database.user,
                password=cfg.database.pw
            )
            db_cursor = db_conn.cursor()
            logging.info("Reconnect Database Again")
        else:
            logging.info("Database connection Alive")
    except Exception as e:
        logging.error(f"Failed to ensure database connection: {e}")
        raise
    return db_conn, db_cursor


def update_database(conn, cursor, table_name, column_name, value, condition, cfg):
    conn, cursor = ensure_db_connection(conn, cursor, cfg)
    try:
        # 세션의 시간대를 KST로 설정
        cursor.execute("SET TIMEZONE = 'Asia/Seoul'")
        query = f"UPDATE {table_name} SET {column_name} = %s, updated_at = NOW() WHERE {condition}"
        cursor.execute(query, (value,))
        conn.commit()
    except Exception as e:
        logging.error(f"Fail update db in {table_name}: {e}")
        conn.rollback()

def insert_into_database(conn, cursor, table_name, column_names, values, cfg):
    conn, cursor = ensure_db_connection(conn, cursor, cfg)
    try:
        cursor.execute("SET TIMEZONE = 'Asia/Seoul'")
        columns = ', '.join(column_names)
        placeholders = ', '.join(['%s'] * len(column_names)) 
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        conn.commit()
    except Exception as e:
        logging.error(f"Fail insert data into {table_name}: {e}")
        conn.rollback()


def get_org_name_from_db(cursor, file_name):
    cursor.execute("SELECT org_name FROM source_file WHERE name = %s", (file_name,))
    org_name = cursor.fetchone()
    if org_name:
        return org_name[0]
    return None

def IndividualFileLocal(chatbot_id, folder_name, files, model, es_index, seq, vendor, es_url):
    logging.info(f"***************************************************************************************") 
    logging.info(f"**********************Start Embedding: {chatbot_id}_{folder_name}**********************")  
    logging.info(f"***************************************************************************************") 
    
    # # env = os.getenv("ENV", "local")
    # cfg_path = f"/workplace/cfg/local_cfg.yaml"
    # cfg = OmegaConf.load(cfg_path)

    # current_dir = Path(__file__).parent

    # # 상위 디렉토리로 이동하여 'cfg' 디렉토리 경로 생성
    # cfg_path = current_dir.parent / 'cfg' / 'local_cfg.yaml'

    ### Load config file
    config_path = os.getenv('CONFIG_PATH', '/workplace/cfg/local_cfg.yaml')
    cfg = OmegaConf.load(config_path) 


    logging.info(f"Local path: {cfg.local.path}")  # 추가: local path 확인
    logging.info(f"Local prefix: {cfg.local.prefix}")  # 추가: local prefix 확인

    results = {
        "files": {
            "excel": [],
            "txt": [],
            "pdf": [],
        },
        "errors": [],
    }
    # Database connection setup
    db_conn = psycopg2.connect(
        host=cfg.database.host,
        port=cfg.database.port,
        dbname=cfg.database.db_name,
        user=cfg.database.user,
        password=cfg.database.pw
    )
    db_cursor = db_conn.cursor()

    try:
        # Extractor, Transformer, Loader, MaumEmbedding 인스턴스 생성
        # Extractor 인스턴스 생성 시 `cfg.local`을 전달합니다.
        if cfg.database.db_schema:
            db_cursor.execute(f'SET search_path TO "{cfg.database.db_schema}";')
        else:
            db_cursor.execute('SET search_path TO service;')

        # if cfg.database.db_schema:
        #     db_cursor.execute(f'SET search_path TO "{cfg.database.db_schema}", service;')

        extractor = Extractor(cfg)
        #TODO : get model from client
        _embedding_model_name = model

        embedding = create_embedding(cfg, _embedding_model_name)
        ### Load config fileC:\Users\seomi\OneDrive\바탕 화면\maumai\rag_agent\amore-agent-rag\cfg
        transformer = Transformer(cfg, embedding, _embedding_model_name, vendor)
        embeddingProxy = MaumEmebedding(cfg, embedding)
        loader = Loader(cfg, es_url)
        local_path = os.path.join(cfg.local.path, f"{chatbot_id}_{folder_name}")
        # local_path = f"{cfg.local.path}/{chatbot_id}_{folder_name}"
        absolute_path = os.path.abspath(local_path)
        logging.info(f"Local path: {local_path}")
        logging.info(f"Absolute path: {absolute_path}")
        logging.info(f"seq num: {seq}")

        # 경로 존재 여부 확인
        if os.path.exists(absolute_path):
            logging.info(f"The path exists: {absolute_path}")
        else:
            logging.info(f"The path does not exist: {absolute_path}")
        normalized_path = os.path.normpath(local_path)
        logging.info(f"Normalized path: {normalized_path}")

        local_prefix = cfg.local.prefix
        # function_name = f"{chatbot_id}_{folder_name}"
        # loader.delete_data_in_group(function_name)

        # files = extractor.get_file_list_from_local(
        #     local_path=local_path, 
        #     prefix=local_prefix,
        #     file_extensions=["pdf", "xls", "xlsx", "txt"]
        # )
        logging.info(f"file_list: {files}")

        # Process Excel files
        excel_file_list = files["xls"] + files["xlsx"]
        for excel_file in excel_file_list:
            logging.info(f"embedding {excel_file}")
            try:
                split_path = str(Path(excel_file).resolve()).split(os.sep)
                group = split_path[-2] if len(split_path) >= 2 else split_path[0]
                logging.info(f"Extracted group name: {group}")
                file_name = os.path.join(local_path, *split_path[-2:])
                extractor.download_file(extractor.local_path, excel_file, file_name)

                if not os.path.exists(file_name) or not file_name.endswith(('.xls', '.xlsx')):
                    raise FileNotFoundError(f"File does not exist or is not an Excel file: {file_name}")

                df = pd.read_excel(file_name, engine='xlrd' if file_name.endswith('.xls') else 'openpyxl')
                ensure_db_connection(db_conn, db_cursor, cfg)

                if df is not None:
                    db_cursor.execute("SELECT id FROM source_file WHERE name = %s", (os.path.basename(excel_file),))
                    source_file_id = db_cursor.fetchone()
                    if source_file_id:
                        source_file_id = source_file_id[0]
                    org_name = get_org_name_from_db(db_cursor, os.path.basename(excel_file))
                    update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "P", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{source_file_id}'", cfg)

                    source_uuid = split_path[-1]
                    metadata_column = []
                    json_column = df.columns
                    total_length = df.shape[0]
                    logging.info(f"Embedding {org_name if org_name else source_uuid}, total length: {total_length}")

                    loader.inplace_docs(source=org_name if org_name else source_uuid, folder_name=f"{chatbot_id}_{folder_name}", es_index=es_index, inplace=True)

                    # loader.inplace_docs(source=source, folder_name=f"{chatbot_id}_{folder_name}", inplace=True)


                    for i in range(0, df.shape[0], 500):
                        df_chunk = df.iloc[i : i + 500, :]
                        logging.info(f"Embedding {org_name if org_name else source_uuid} {i} ~ {i+500}")
                        docs = transformer.create_json_document_from_dataframe(
                            df=df_chunk,
                            source=org_name if org_name else source_uuid,
                            metadata_columns=metadata_column,
                            json_columns=json_column,
                            model=model
                        )
                        for doc in docs:
                            doc.metadata["group"] = group
                            doc.metadata["source_uuid"] = source_uuid
                            doc.metadata["source"] = org_name if org_name else source_uuid
                        loader.load_bulk(docs, es_index=es_index, model=model)
                    results["files"]["excel"].append(file_name)
                    update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{source_file_id}'", cfg)

                    # update_database(db_conn, db_cursor, "function_file", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND file_id = '{source_file_id}'")
                    # update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{source_file_id}'")
                else:
                    raise ValueError(f"Failed to read Excel file: {file_name}")


            except Exception as e:
                logging.error(f"Error processing Excel file {excel_file}: {e}")
                results["errors"].append({"file": excel_file, "error": str(e)})
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "E", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{source_file_id}'", cfg)


        # Process TXT files
        txt_file_list = files["txt"]
        for txt_file in txt_file_list:
            try:
                with open(txt_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                doc = Document(page_content=content, metadata={"source": txt_file})
                docs = [doc]
                txt_docs = []
                ensure_db_connection(db_conn, db_cursor, cfg)
                db_cursor.execute("SELECT id FROM source_file WHERE name = %s", (os.path.basename(txt_file),))
                txt_source_file_id = db_cursor.fetchone()

                if txt_source_file_id:
                    txt_source_file_id = txt_source_file_id[0]

                org_name = get_org_name_from_db(db_cursor, os.path.basename(txt_file))
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "P", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{txt_source_file_id}'", cfg)

                for doc in docs:
                    split_docs = transformer.text_splitter.split_documents([doc])
                    for split_doc in split_docs:
                        split_doc.page_content = preprocessing(split_doc.page_content)
                        vector = embeddingProxy.embed_query(split_doc.page_content)
                        vector_name = "vertor"
                        if model == "jhgan/ko-sroberta-multitask":
                            vector_name = "vector-ko-sroberta-multitask"
                        elif model == "intfloat/multilingual-e5-large-instruct":
                            vector_name = "vector-multilingual-e5-large-instruct"
                        split_doc.metadata[vector_name] = vector
                        split_doc.metadata["uuid"] = uuid.uuid4().hex
                        txt_path = str(Path(txt_file).resolve())
                        split_doc.metadata["source"] = org_name if org_name else txt_path.split(os.sep)[-1]
                        split_doc.metadata["source_uuid"] = txt_path.split(os.sep)[-1]
                        # split_doc.metadata["source"] = txt_path.split(os.sep)[-1]
                        if len(txt_path.split(os.sep)) > 1:
                            split_doc.metadata["group"] = txt_path.split(os.sep)[-2]
                        else:
                            split_doc.metadata["group"] = txt_path
                        txt_docs.append(split_doc)
                        # engine을 사용한 embedding api 호출시 db에 token 수 저장
                        use_tokens = transformer.get_token_count(vendor, split_doc.page_content)
                        db_column_name = ["log_id", "room_id", "seq", "model", "tokens", "token_type", "engine_type", "service_type"]
                        db_values = [chatbot_id, folder_name, seq, model, use_tokens, "I", "EMBED", "EMBEDDING"]
                        insert_into_database(db_conn, db_cursor, "stat_chathub_raw", db_column_name, db_values, cfg)
                if txt_docs:
                    source = txt_docs[0].metadata["source"]

                    loader.inplace_docs(source=source,folder_name=f"{chatbot_id}_{folder_name}", es_index=es_index, inplace=True)
                    loader.load_bulk(docs=txt_docs, es_index=es_index, model=model)
                    results["files"]["txt"].append(txt_file)
                    # update_database(db_conn, db_cursor, "function_file", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND file_id = '{txt_source_file_id}'")
                    # update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{txt_source_file_id}'")
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{txt_source_file_id}'", cfg)

            except Exception as e:
                logging.error(f"Error processing TXT file {txt_file}: {e}")
                results["errors"].append({"file": txt_file, "error": str(e)})
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "E", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{txt_source_file_id}'", cfg)

        # Process PDF files
        pdf_file_list = files["pdf"]
        for pdf_file in pdf_file_list:
            try:
                logging.info(f"embedding {pdf_file}")
                file_size = os.path.getsize(pdf_file)
                logging.info(f"파일 크기 {file_size}")                
                pdf_loader = PyPDFLoader(pdf_file)
                logging.info("pdf_loader end")                
                # pdf_docs = pdf_loader.load_and_split(text_splitter=transformer.text_splitter)
                if file_size < 10 * 1024 * 1024:
                    pdf_docs = pdf_loader.load_and_split(text_splitter=transformer.text_splitter)
                else: 
                    pdf_docs = pdf_loader.lazy_load() 
                    pdf_docs = transformer.text_splitter.split_documents(pdf_docs) 
                logging.info("text_splitter end")                
                ensure_db_connection(db_conn, db_cursor, cfg)
                db_cursor.execute("SELECT id FROM source_file WHERE name = %s", (os.path.basename(pdf_file),))
                pdf_source_file_id = db_cursor.fetchone()

                if pdf_source_file_id:
                    pdf_source_file_id = pdf_source_file_id[0]
                org_name = get_org_name_from_db(db_cursor, os.path.basename(pdf_file))
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "P", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{pdf_source_file_id}'", cfg)

                for doc in pdf_docs:
                    doc.page_content = preprocessing(doc.page_content)
                    vector = embeddingProxy.embed_query(doc.page_content)
                    vector_name = "vertor"
                    if model == "jhgan/ko-sroberta-multitask":
                       vector_name = "vector-ko-sroberta-multitask"
                    elif model == "intfloat/multilingual-e5-large-instruct":
                        vector_name = "vector-multilingual-e5-large-instruct"
                    doc.metadata[vector_name] = vector
                    doc.metadata["uuid"] = uuid.uuid4().hex
                    pdf_path = str(Path(pdf_file).resolve())
                    doc.metadata["source"] = org_name if org_name else pdf_path.split(os.sep)[-1]
                    doc.metadata["source_uuid"] = pdf_path.split(os.sep)[-1]
                    # doc.metadata["source"] = pdf_path.split(os.sep)[-1]
                    if len(pdf_path.split(os.sep)) > 1:
                        doc.metadata["group"] = pdf_path.split(os.sep)[-2]
                    else:
                        doc.metadata["group"] = pdf_path
                    
                    # engine을 사용한 embedding api 호출시 db에 token 수 저장
                    use_tokens = transformer.get_token_count(vendor, doc.page_content)
                    db_column_name = ["log_id", "room_id","seq", "model", "tokens", "token_type", "engine_type", "service_type"]
                    db_values = [chatbot_id, folder_name, seq, model, use_tokens, "I", "EMBED", "EMBEDDING"]
                    insert_into_database(db_conn, db_cursor, "stat_chathub_raw", db_column_name, db_values, cfg)
                ensure_db_connection(db_conn, db_cursor, cfg)
                source = pdf_docs[0].metadata["source"]
                loader.inplace_docs(source=source,  folder_name=f"{chatbot_id}_{folder_name}", es_index=es_index, inplace=True)
                loader.load_bulk(docs=pdf_docs, es_index=es_index, model=model)
                results["files"]["pdf"].append(pdf_file)
                # update_database(db_conn, db_cursor, "function_file", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND file_id = '{pdf_source_file_id}'")
                # update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{pdf_source_file_id}'")
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{pdf_source_file_id}'", cfg)

            except Exception as e:
                logging.error(f"Error processing PDF file {pdf_file}: {e}")
                results["errors"].append({"file": pdf_file, "error": str(e)})
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "E", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{pdf_source_file_id}'", cfg)



        # for file_type in results["files"]:
        #     for file_data in results["files"][file_type]:
        #         logging.info(f'파일 명은 {file_data}')
        #         db_cursor.execute("SELECT id FROM source_file WHERE name = %s", (os.path.basename(file_data),))
        #         source_file_id = db_cursor.fetchone()
        #         if source_file_id:
        #             source_file_id = source_file_id[0]
            
        #         update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{source_file_id}'")
        ensure_db_connection(db_conn, db_cursor, cfg)
        db_cursor.execute("SELECT file_id, embedding_status FROM chatbot_info_detail_embedding_status WHERE chatbot_id = %s AND function_id = %s", (chatbot_id, folder_name))
        all_files_status = db_cursor.fetchall()

        for file_id, embedding_status in all_files_status:
            if embedding_status != "C":
                update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "E", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{file_id}'", cfg)
                logging.info(f"Updated embedding_status to 'E' for file_id {file_id}")

        db_cursor.execute(f"SELECT embedding_status FROM chatbot_info_detail_embedding_status WHERE chatbot_id = %s", (chatbot_id,))
        embedding_statuses = db_cursor.fetchall()
        statuses = [status[0] for status in embedding_statuses]
        has_C = "C" in statuses  
        all_E = all(status == "E" for status in statuses) 
        has_T_or_P = "T" in statuses or "P" in statuses

        logging.info(f"Statuses for chatbot_id {chatbot_id}: {statuses}")
        if has_T_or_P:
            logging.info(f"Skipping update for chatbot_id {chatbot_id} due to pending 'T' or 'P' status")
        else:
            if has_C:
                update_database(db_conn, db_cursor, "chatbot_info", "embedding_status", "C", f"id = '{chatbot_id}'", cfg)
                logging.info(f"Updated chatbot_info embedding_status to 'C' for chatbot_id {chatbot_id}")
            elif all_E:
                update_database(db_conn, db_cursor, "chatbot_info", "embedding_status", "E", f"id = '{chatbot_id}'", cfg)
                logging.info(f"Updated chatbot_info embedding_status to 'E' for chatbot_id {chatbot_id}")

        # update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", "C", f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}' AND file_id = '{source_file_id}'")

        # has_files = any(results["files"][file_type] for file_type in results["files"])

        # if has_files:
        #     update_value = "C"
        # else:
        #     update_value = "E"

        # update_database(db_conn, db_cursor, "chatbot_info_detail_embedding_status", "embedding_status", update_value, f"chatbot_id = '{chatbot_id}' AND function_id = '{folder_name}'")

        # db_cursor.execute(f"SELECT COUNT(*) FROM chatbot_info_detail_embedding_status WHERE chatbot_id = %s AND embedding_status != 'C'", (chatbot_id,))
        # not_completed_count = db_cursor.fetchone()[0]
        # logging.info(f'not completed files num: {not_completed_count}')

        # if not_completed_count == 0:
        #     update_database(db_conn, db_cursor, "chatbot_info", "embedding_status", "C", f"id = '{chatbot_id}'")

        # update_database(db_conn, db_cursor, "base_member_function", "embedding_status", update_value, f"id = '{folder_name}'")

    except Exception as e:
        logging.error(f"Error in IndividualFileLocal function: {e}")
        results["errors"].append({"file": "all", "error": str(e)})
    finally:
        logging.info(f"***************************************************************************************") 
        logging.info(f"***********************End Embedding: {chatbot_id}_{folder_name}***********************")  
        logging.info(f"***************************************************************************************") 
        db_cursor.close()
        db_conn.close()

    return results


if __name__ == "__main__":
    IndividualFileLocal()
