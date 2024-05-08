import logging
import os
from pathlib import Path
import uuid
from langchain.document_loaders import S3FileLoader, TextLoader
from omegaconf import OmegaConf
from extractor import Extractor
from transformer import Transformer
from loader import Loader
# from langchain.document_loaders import S3FileLoader
import pandas as pd
from langchain.schema import Document


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

def main():
    ### set logger
    logging.basicConfig(level=logging.INFO)
    # # env = os.getenv("ENV", "local")
    # cfg_path = f"/workplace/cfg/local_cfg.yaml"
    # cfg = OmegaConf.load(cfg_path)

    # current_dir = Path(__file__).parent

    # # 상위 디렉토리로 이동하여 'cfg' 디렉토리 경로 생성
    # cfg_path = current_dir.parent / 'cfg' / 'local_cfg.yaml'

    # # 파일 존재 여부 확인
    # if not cfg_path.exists():
    #     raise FileNotFoundError(f"The configuration file '{cfg_path}' does not exist.")
    # print(cfg_path)
    ### Load config file
    cfg = OmegaConf.load(str(r'C:\Users\seomi\OneDrive\바탕 화면\maumai\rag_agent\amore-agent-rag\cfg\local_cfg.yaml'))
    print(f"Type of cfg: {type(cfg)}")  # 추가: cfg의 타입 확인
    print(f"Type of cfg.local: {type(cfg.local)}")  # 추가: cfg.local의 타입 확인
    print(f"Local path: {cfg.local.path}")  # 추가: local path 확인
    print(f"Local prefix: {cfg.local.prefix}")  # 추가: local prefix 확인

    # Extractor 인스턴스 생성 시 `cfg.local`을 전달합니다.
    extractor = Extractor(cfg) 
    ### Load config fileC:\Users\seomi\OneDrive\바탕 화면\maumai\rag_agent\amore-agent-rag\cfg
    # /Users/seomi/OneDrive/바탕 화면/maumai/rag_agent
    # cfg_path = os.environ.get("CONFIG_PATH", "../cfg/local_cfg.yml") 
    # if not os.path.exists(cfg_path):
    #     raise FileNotFoundError(f"The configuration file '{cfg_path}' does not exist.")
    # cfg = OmegaConf.load(cfg_path)



    extractor = Extractor(cfg)
    transformer = Transformer(cfg)
    loader = Loader(cfg)

    local_path = cfg.local.path
    local_prefix = cfg.local.prefix

    files = extractor.get_file_list_from_local(
        local_path=local_path, 
        prefix=local_prefix,
        file_extensions=["pdf", "xls", "xlsx", "txt"]
    )
    logging.info(f"file_list: {files}")

    # excel_file_list = files["xlsx"] + files["xls"]
    # for excel_file in excel_file_list:
    #     logging.info(f"embedding {excel_file}")
    #     split_path = excel_file.split(os.sep)
    #     if len(split_path) >= 2:
    #         group = split_path[-2]
    #     else:
    #         group = split_path[0]
    #         print(f"Unexpected path structure. Using file name as group: {group}")
    #     print(f"Extracted group name: {group}")
        
    #     # group = excel_file.split(os.sep)[-2]
    #     # print(f"Extracted group name: {group}")

    #     file_name = os.path.join("/workplace/", excel_file.split(os.sep)[-1])
    #     extractor.download_file(local_path, excel_file, file_name)

    #     df = pd.read_excel(file_name)
    #     source = file_name.split(os.sep)[-1]

    #     metadata_column = []
    #     json_column = df.columns
    #     total_length = df.shape[0]
    #     logging.info(f"embedding {source}, total length: {total_length}")
    #     loader.inplace_docs(source=source, inplace=True)
    #     for i in range(0, df.shape[0], 500):
    #         df_chunk = df.iloc[i : i + 500, :]
    #         logging.info(f"embedding {source} {i} ~ {i+500}")
    #         docs = transformer.create_json_document_from_dataframe(
    #             df=df_chunk,
    #             source=source,
    #             metadata_columns=metadata_column,
    #             json_columns=json_column,
    #         )
    #         for doc in docs:
    #             doc.metadata["group"] = group
    #         ### load data to es
    #         loader.load_bulk(docs)

    # txt_file_list = files["txt"]
    # for txt_file in txt_file_list:
    #     logging.info(f"embedding {txt_file}")
    #     text_loader = TextLoader(txt_file)
    #     txt_docs = text_loader.load_and_split(text_splitter=transformer.text_splitter)

    #     for doc in txt_docs:
    #         doc.page_content = preprocessing(doc.page_content)
    #         vector = transformer.embedding.embed_query(doc.page_content)
    #         source = doc.metadata["source"]
    #         doc.metadata["vector"] = vector
    #         doc.metadata["uuid"] = uuid.uuid4().hex
    #         doc.metadata["source"] = source.split(os.sep)[-1]
    #         doc.metadata["group"] = source.split(os.sep)[-2]

    #     source = txt_docs[0].metadata["source"]
    #     loader.inplace_docs(source=source, inplace=True)
    #     loader.load_bulk(docs=txt_docs)

    # ### Use Langchain to get PDF load and split
    # pdf_file_list = files["pdf"]
    # for pdf_file in pdf_file_list:
    #     logging.info(f"embedding {pdf_file}")
    #     pdf_loader = TextLoader(pdf_file)
    #     pdf_docs = pdf_loader.load_and_split(text_splitter=transformer.text_splitter)

    #     for doc in pdf_docs:
    #         doc.page_content = preprocessing(doc.page_content)
    #         vector = transformer.embedding.embed_query(doc.page_content)
    #         source = doc.metadata["source"]
    #         doc.metadata["vector"] = vector
    #         doc.metadata["uuid"] = uuid.uuid4().hex
    #         doc.metadata["source"] = source.split(os.sep)[-1]
    #         doc.metadata["group"] = source.split(os.sep)[-2]

    #     source = pdf_docs[0].metadata["source"]
    #     loader.inplace_docs(source=source, inplace=True)
    #     loader.load_bulk(docs=pdf_docs)
    excel_file_list = files["xls"] + files["xlsx"]
    for excel_file in excel_file_list:
        logging.info(f"embedding {excel_file}")

        split_path = excel_file.split(os.sep)
        if len(split_path) >= 2:
            group = split_path[-2]
        else:
            group = split_path[0]
            logging.warning(f"Unexpected path structure. Using file name as group: {group}")

        print(f"Extracted group name: {group}")

        # Adjust file name for processing
        file_name = os.path.join("/workplace/", split_path[-1])
        extractor.download_file(extractor.local_path, excel_file, file_name)

        # Read Excel file and perform necessary actions
        # df = read_excel_file(file_name)
        df = pd.read_excel(file_name)
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
                ### load data to es
                loader.load_bulk(docs)
        else:
            logging.error(f"Failed to read Excel file: {file_name}")

    # Process text files
    txt_file_list = files["txt"]
    for txt_file in txt_file_list:
        try:
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError as e:
            logging.error(f"Error loading {txt_file} with UTF-8 encoding: {e}")
            continue  # 인코딩 오류가 발생한 파일은 건너뜁니다.

        # 파일 내용을 Document 형식으로 변환
        doc = Document(page_content=content, metadata={"source": txt_file})
        docs = [doc]
        txt_docs = []
        for doc in docs:
            split_docs = transformer.text_splitter.split_documents([doc])
            for split_doc in split_docs:
                split_doc.page_content = preprocessing(split_doc.page_content)
                vector = transformer.embedding.embed_query(split_doc.page_content)
                split_doc.metadata["vector"] = vector
                split_doc.metadata["uuid"] = uuid.uuid4().hex
                split_doc.metadata["source"] = txt_file.split(os.sep)[-1]
                if len(txt_file.split(os.sep)) > 1:
                    split_doc.metadata["group"] = txt_file.split(os.sep)[-2]
                else:
                    split_doc.metadata["group"] = txt_file
                txt_docs.append(split_doc)

        if txt_docs:
            source = txt_docs[0].metadata["source"]
            loader.inplace_docs(source=source, inplace=True)
            loader.load_bulk(docs=txt_docs)
        # logging.info(f"embedding {txt_file}")
        # text_loader = TextLoader(txt_file)
        # txt_docs = text_loader.load_and_split(text_splitter=transformer.text_splitter)

        # for doc in txt_docs:
        #     doc.page_content = preprocessing(doc.page_content)
        #     vector = transformer.embedding.embed_query(doc.page_content)
        #     source = doc.metadata["source"]
        #     doc.metadata["vector"] = vector
        #     doc.metadata["uuid"] = uuid.uuid4().hex
        #     doc.metadata["source"] = source.split(os.sep)[-1]
        #     if len(source.split(os.sep)) > 1:
        #         doc.metadata["group"] = source.split(os.sep)[-2]
        #     else:
        #         doc.metadata["group"] = source  # Default group if path structure is not as expected

        # source = txt_docs[0].metadata["source"]
        # loader.inplace_docs(source=source, inplace=True)
        # loader.load_bulk(docs=txt_docs)

    # Process PDF files
    pdf_file_list = files["pdf"]
    for pdf_file in pdf_file_list:
        logging.info(f"embedding {pdf_file}")
        # pdf_loader = S3FileLoader(bucket=local_path, key = pdf_file)
        pdf_loader = TextLoader(pdf_file)
        pdf_docs = pdf_loader.load_and_split(text_splitter=transformer.text_splitter)

        for doc in pdf_docs:
            doc.page_content = preprocessing(doc.page_content)
            vector = transformer.embedding.embed_query(doc.page_content)
            source = doc.metadata["source"]
            doc.metadata["vector"] = vector
            doc.metadata["uuid"] = uuid.uuid4().hex
            doc.metadata["source"] = source.split(os.sep)[-1]
            if len(source.split(os.sep)) > 1:
                doc.metadata["group"] = source.split(os.sep)[-2]
            else:
                doc.metadata["group"] = source  # Default group if path structure is not as expected

        source = pdf_docs[0].metadata["source"]
        loader.inplace_docs(source=source, inplace=True)
        loader.load_bulk(docs=pdf_docs)

if __name__ == "__main__":
    main()