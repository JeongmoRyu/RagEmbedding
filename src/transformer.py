import logging
import os
import uuid
from typing import Dict, List, Optional

import pandas as pd
import tiktoken
from langchain.docstore.document import Document
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.text_splitter import TokenTextSplitter
from omegaconf import OmegaConf


class Transformer:
    """
    Transform data to documents for elastic search
    Using Langchain Document format
    """

    def __init__(self, cfg):
        self.cfg = OmegaConf.load(cfg)["embedding"]
        os.environ["OPENAI_API_KEY"] = self.cfg.openai_api_key
        os.environ["OPENAI_API_VERSION"] = self.cfg.openai_api_version
        self.embedding = OpenAIEmbeddings(
            deployment=self.cfg.deployment,
            model=self.cfg.deployment,
            openai_api_base=self.cfg.openai_base_url,
            openai_api_type="azure",
            openai_api_key=self.cfg.openai_api_key,
            openai_api_version=self.cfg.openai_api_version,
            request_timeout=60,
            max_retries=1000,
            retry_min_seconds=30,
            retry_max_seconds=60,
        )
        self.text_splitter = TokenTextSplitter(
            chunk_size=self.cfg.chunk_size, chunk_overlap=self.cfg.chunk_overlap
        )
        self.encoder = tiktoken.get_encoding("cl100k_base")

    ### WILL BE DEPRECATED
    def _add_source_to_text(
        self, source: str, text: str, remove_single_quote: bool = True
    ) -> str:
        if remove_single_quote:
            return f"source= {source} text= {text}".replace("'", "")
        else:
            return f"source= {source} text= {text}"

    def _delete_empty_string_from_dict(self, data: Dict) -> Dict:
        """Delete value with empty space string"""
        return {
            k: v
            for k, v in data.items()
            if (isinstance(v, str) and len(v.strip(" "))) or isinstance(v, (int, float))  > 0
        }

    def _split_from_dict(self, data: Dict, text_splitter) -> List[Dict]:
        """
        chunk dictionary value based on text splitter's limit length

        Args:
            data: Dictionary to split
            text_splitter: text splitter to use

        Returns:
            List[Dict]:
        """
        docs = []
        token_len = 0
        tmp_dict = dict()
        for key, value in data.items():
            text = f"{key}:{value}"
            if len(self.encoder.encode(text)) >= self.cfg.chunk_size:
                text_chunks = text_splitter.split_text(text)
                for n, chunk in enumerate(text_chunks):
                    docs.append({f"{key}_{n}": chunk})
            else:
                if token_len + len(self.encoder.encode(text)) < self.cfg.chunk_size:
                    token_len += len(self.encoder.encode(text))
                    tmp_dict[key] = value
                else:
                    if len(self.encoder.encode(text)) >= self.cfg.chunk_size:
                        text_chunks = text_splitter.split_text(text)
                        for n, chunk in enumerate(text_chunks):
                            docs.append({f"{key}_{n}": chunk})
                    else:
                        docs.append({key: value})
                    docs.append(tmp_dict)
                    tmp_dict = {}
                    token_len = 0

        if tmp_dict:
            docs.append(tmp_dict)

        return docs

    ### TODO: add description
    def _validate_data(
        self,
        df: pd.DataFrame,
        json_columns: List[str],
        embedding_column: Optional[str] = None,
    ) -> bool:
        """
        SAMPLE BLABLA

        Args:
            df:
            json_columns:
            embedding_column:

        Returns:
            bool
        """
        if embedding_column:
            if df[json_columns].isna().all() or df[embedding_column].isna().any():
                return False
            else:
                return True
        else:
            if df[json_columns].isna().all():
                return False
            else:
                return True

    def get_text_file_in_chunk(
        self, text_file, text_splitter
    ):
        loader = TextLoader(text_file)
        text_file = loader.load_and_split(text_splitter=text_splitter)
        for doc in text_file:
            doc.metadata["uuid"] = uuid.uuid4().hex
        return text_file
    
    def create_json_document_from_dataframe(
        self,
        df: pd.DataFrame,
        source: str,
        metadata_columns: List[str],
        json_columns: List[str],
        embedding_column: str = None,  ### only one plz
        embedding=None,
        text_splitter=None,
        stem_columns: List[str] = None,
    ) -> List[Document]:
        """
        create json document from dataframe

        Args:
            df: data to preprocess
            metadata_columns: metadata columns
            stem_columns: if text_splitter is not None, stem will be reproduced to every docs which is seperated by text_splitter
            json_columns: json columns
            embedding: embedding functions to use
            text_splitter: text splitter to use

        Returns:
            List[Document]: List of Documents to load in ES
        """
        logging.info(f"Embedding {source} in json format")
        logging.info(f"metadata columns : {metadata_columns}")
        logging.info(f"content columns : {json_columns}")

        if not embedding:
            embedding = self.embedding

        docs = []
        for i in range(0, df.shape[0]):
            row = df.iloc[i, :]

            if not self._validate_data(
                df=row,
                json_columns=json_columns,
                embedding_column=embedding_column
            ):
                continue

            metadata = self._delete_empty_string_from_dict(
                row[metadata_columns].dropna().to_dict()
            )
            metadata["uuid"] = uuid.uuid4().hex
            metadata["source"] = source
            contents = self._delete_empty_string_from_dict(
                row[json_columns].dropna().to_dict()
            )

            if text_splitter:
                json_contents = self._split_from_dict(contents, text_splitter)

                if stem_columns:
                    stem_contents = self._delete_empty_string_from_dict(
                        row[stem_columns].dropna().to_dict()
                    )
                    json_contents = [
                        dict(stem_contents, **json_content)
                        for json_content in json_contents
                    ]

            else:
                json_contents = [contents]
                if stem_columns:
                    stem_contents = self._delete_empty_string_from_dict(
                        row[stem_columns].dropna().to_dict()
                    )
                    json_contents = [
                        dict(stem_contents, **json_content)
                        for json_content in json_contents
                    ]

            for n, json_content in enumerate(json_contents):
                _metadata = metadata.copy()
                _metadata["index"] = n
                text = (
                    str({f"```{k}```": v for k, v in json_content.items()})
                    .replace("'", "")
                    .replace("{", "")
                    .replace("}", "")
                )
                if not embedding_column:
                    vector = embedding.embed_query(str(json_content))
                else:
                    try:
                        vector = embedding.embed_query(
                            str(row[embedding_column].to_dict())
                        )
                    except Exception:
                        logging.info(f"row doesn't have {embedding_column}")
                        continue
                _metadata["vector"] = vector
                doc = Document(
                    page_content=text,
                    metadata=_metadata,
                )
                docs.append(doc)

        return docs
