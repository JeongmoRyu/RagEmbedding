from extractor import Extractor
from transformer import Transformer
from loader import Loader
import os
from langchain.document_loaders import S3FileLoader
import pandas as pd
from omegaconf import OmegaConf
import logging
import uuid

def preprocessing(text: str):
    return text.replace("~", "-").replace("\n", "")

def main():
    ### set logger
    logging.basicConfig(level=logging.INFO)

    ### Load config file
    env = os.environ["ENV"]
    cfg = f"/workplace/cfg/{env}_cfg.yaml"

    ### Call Classes to use
    extractor = Extractor(cfg)
    transformer = Transformer(cfg)
    loader = Loader(cfg)

    cfg = OmegaConf.load(cfg)

    ### Get File lists to embed
    files = extractor.get_file_list_from_s3(
        bucket=cfg.s3.bucket, prefix=cfg.s3.prefix, file_extensions=["pdf", "xls", "xlsx", "txt"]
    )
    logging.info(f"file_list: {files}")

    ### Excel File ETL
    excel_file_list = files["xlsx"]
    excel_file_list.extend(files["xls"])
    for excel_file in excel_file_list:
        logging.info(f"embedding {excel_file}")
        group = excel_file.split("/")[-2]
        file_name = os.path.join("/workplace/", excel_file.split("/")[-1])
        extractor.download_file(cfg.s3.bucket, excel_file, file_name)

        df = pd.read_excel(file_name)
        source = file_name.split("/")[-1]

        metadata_column = []
        json_column = df.columns
        total_length = df.shape[0]
        logging.info(f"embedding {source}, total length: {total_length}")
        loader.inplace_docs(source=source, inplace=True)
        for i in range(0, df.shape[0], 500):
            df_chunk = df.iloc[i : i + 500, :]
            logging.info(f"emebdding {source} {i} ~ {i+500}")
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

    txt_file_list = files["txt"]
    for txt_file in txt_file_list:
        logging.info(f"embedding {txt_file}")
        txt_loader = S3FileLoader(bucket=cfg.s3.bucket, key=txt_file)
        txt_docs = txt_loader.load_and_split(text_splitter=transformer.text_splitter)

        for doc in txt_docs:
            doc.page_content = preprocessing(doc.page_content)
            vector = transformer.embedding.embed_query(doc.page_content)
            source = doc.metadata["source"]
            doc.metadata["vector"] = vector
            doc.metadata["uuid"] = uuid.uuid4().hex
            doc.metadata["source"] = source.split("/")[-1]
            doc.metadata["group"] = source.split("/")[-2]

        source = txt_docs[0].metadata["source"]
        loader.inplace_docs(source=source, inplace=True)
        loader.load_bulk(docs=txt_docs)

    ### Use Langchain to get PDF load and split
    pdf_file_list = files["pdf"]
    for pdf_file in pdf_file_list:
        logging.info(f"embedding {pdf_file}")
        pdf_loader = S3FileLoader(bucket=cfg.s3.bucket, key=pdf_file)
        pdf_docs = pdf_loader.load_and_split(text_splitter=transformer.text_splitter)

        for doc in pdf_docs:
            doc.page_content = preprocessing(doc.page_content)
            vector = transformer.embedding.embed_query(doc.page_content)
            source = doc.metadata["source"]
            doc.metadata["vector"] = vector
            doc.metadata["uuid"] = uuid.uuid4().hex
            doc.metadata["source"] = source.split("/")[-1]
            doc.metadata["group"] = source.split("/")[-2]

        source = pdf_docs[0].metadata["source"]
        loader.inplace_docs(source=source, inplace=True)
        loader.load_bulk(docs=pdf_docs)

if __name__ == "__main__":
    main()