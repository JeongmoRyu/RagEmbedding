import logging
import time
import os
from pathlib import Path
import uuid
from langchain.document_loaders import S3FileLoader, TextLoader, PyPDFLoader
from omegaconf import OmegaConf
from extractor import Extractor
from transformer import Transformer
from loader import Loader
import pandas as pd
# from langchain.document_loaders import S3FileLoader
from pathlib import Path
from langchain.schema import Document
# import sys


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

def IndividualFileLocal(folder_name):
    # if len(sys.argv) > 1:
    #   folder_name = sys.argv[1]
    #   print(f"Received folder name: {folder_name}")
    # else:
    #   print("No folder name provided.")
    # ### set logger
    # logging.basicConfig(level=logging.INFO)

    
    # # env = os.getenv("ENV", "local")
    # cfg_path = f"/workplace/cfg/local_cfg.yaml"
    # cfg = OmegaConf.load(cfg_path)

    # current_dir = Path(__file__).parent

    # # 상위 디렉토리로 이동하여 'cfg' 디렉토리 경로 생성
    # cfg_path = current_dir.parent / 'cfg' / 'local_cfg.yaml'
    ### Load config file
    cfg = OmegaConf.load(str(r'../cfg/local_cfg.yaml'))
    # cfg = OmegaConf.load(str(r'C:\\Users\\maum.ai\\Desktop\\AP\\AIBC(mcl)-mStudio\\amore-agent-rag\\cfg\\local_cfg.yaml'))
    print(f"Type of cfg: {type(cfg)}")  # 추가: cfg의 타입 확인
    print(f"Type of cfg.local: {type(cfg.local)}")  # 추가: cfg.local의 타입 확인
    print(f"Local path: {cfg.local.path}")  # 추가: local path 확인
    print(f"Local prefix: {cfg.local.prefix}")  # 추가: local prefix 확인

    results = {
        "files": {
            "excel": [],
            "txt": [],
            "pdf": [],
        },
        "errors": [],
    }

    try:
        # Extractor, Transformer, Loader 인스턴스 생성
        # Extractor 인스턴스 생성 시 `cfg.local`을 전달합니다.

        extractor = Extractor(cfg)
        ### Load config fileC:\Users\seomi\OneDrive\바탕 화면\maumai\rag_agent\amore-agent-rag\cfg
        transformer = Transformer(cfg)
        loader = Loader(cfg)

        local_path = f"{cfg.local.path}/{folder_name}"
        print(local_path)
        local_prefix = cfg.local.prefix

        files = extractor.get_file_list_from_local(
            local_path=local_path, 
            prefix=local_prefix,
            file_extensions=["pdf", "xls", "xlsx", "txt"]
        )
        logging.info(f"file_list: {files}")

        # Process Excel files
        excel_file_list = files["xls"] + files["xlsx"]
        for excel_file in excel_file_list:
            logging.info(f"embedding {excel_file}")
            try:
                split_path = str(Path(excel_file).resolve()).split(os.sep)
                group = split_path[-2] if len(split_path) >= 2 else split_path[0]
                print(f"Extracted group name: {group}")
                file_name = os.path.join(local_path, *split_path[-2:])
                extractor.download_file(extractor.local_path, excel_file, file_name)

                if not os.path.exists(file_name) or not file_name.endswith(('.xls', '.xlsx')):
                    raise FileNotFoundError(f"File does not exist or is not an Excel file: {file_name}")

                df = pd.read_excel(file_name, engine='xlrd' if file_name.endswith('.xls') else 'openpyxl')

                if df is not None:
                    source = split_path[-1]
                    metadata_column = []
                    json_column = df.columns
                    total_length = df.shape[0]
                    logging.info(f"embedding {source}, total length: {total_length}")
                    loader.inplace_docs(source=source, inplace=True)
                    for i in range(0, df.shape[0], 500):
                        df_chunk = df.iloc[i : i + 500, :]
                        logging.info(f"embedding {source} {i} ~ {i+500}")
                        docs = transformer.create_json_document_from_dataframe(
                            df=df_chunk,
                            source=source,
                            metadata_columns=metadata_column,
                            json_columns=json_column,
                        )
                        for doc in docs:
                            doc.metadata["group"] = group
                        loader.load_bulk(docs)
                    results["files"]["excel"].append(file_name)
                else:
                    raise ValueError(f"Failed to read Excel file: {file_name}")

            except Exception as e:
                logging.error(f"Error processing Excel file {excel_file}: {e}")
                results["errors"].append({"file": excel_file, "error": str(e)})

        # Process TXT files
        txt_file_list = files["txt"]
        for txt_file in txt_file_list:
            try:
                with open(txt_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                doc = Document(page_content=content, metadata={"source": txt_file})
                docs = [doc]
                txt_docs = []
                for doc in docs:
                    split_docs = transformer.text_splitter.split_documents([doc])
                    for split_doc in split_docs:
                        split_doc.page_content = preprocessing(split_doc.page_content)
                        vector = transformer.embed_query(split_doc.page_content)
                        split_doc.metadata["vector"] = vector
                        split_doc.metadata["uuid"] = uuid.uuid4().hex
                        txt_path = str(Path(txt_file).resolve())
                        split_doc.metadata["source"] = txt_path.split(os.sep)[-1]
                        if len(txt_path.split(os.sep)) > 1:
                            split_doc.metadata["group"] = txt_path.split(os.sep)[-2]
                        else:
                            split_doc.metadata["group"] = txt_path
                        txt_docs.append(split_doc)
                if txt_docs:
                    source = txt_docs[0].metadata["source"]
                    loader.inplace_docs(source=source, inplace=True)
                    loader.load_bulk(docs=txt_docs)
                    results["files"]["txt"].append(txt_file)
            except Exception as e:
                logging.error(f"Error processing TXT file {txt_file}: {e}")
                results["errors"].append({"file": txt_file, "error": str(e)})

        # Process PDF files
        pdf_file_list = files["pdf"]
        for pdf_file in pdf_file_list:
            try:
                logging.info(f"embedding {pdf_file}")
                pdf_loader = PyPDFLoader(pdf_file)
                pdf_docs = pdf_loader.load_and_split(text_splitter=transformer.text_splitter)
                for doc in pdf_docs:
                    doc.page_content = preprocessing(doc.page_content)
                    vector = transformer.embed_query(doc.page_content)
                    doc.metadata["vector"] = vector
                    doc.metadata["uuid"] = uuid.uuid4().hex
                    pdf_path = str(Path(pdf_file).resolve())
                    doc.metadata["source"] = pdf_path.split(os.sep)[-1]
                    if len(pdf_path.split(os.sep)) > 1:
                        doc.metadata["group"] = pdf_path.split(os.sep)[-2]
                    else:
                        doc.metadata["group"] = pdf_path
                source = pdf_docs[0].metadata["source"]
                loader.inplace_docs(source=source, inplace=True)
                loader.load_bulk(docs=pdf_docs)
                results["files"]["pdf"].append(pdf_file)
            except Exception as e:
                logging.error(f"Error processing PDF file {pdf_file}: {e}")
                results["errors"].append({"file": pdf_file, "error": str(e)})

    except Exception as e:
        logging.error(f"Error in IndividualFileLocal function: {e}")
        results["errors"].append({"file": "all", "error": str(e)})

    return results

if __name__ == "__main__":
    IndividualFileLocal()
