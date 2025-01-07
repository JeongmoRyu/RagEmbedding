import logging
import time
from omegaconf import OmegaConf
from typing import List
from elasticsearch import Elasticsearch, helpers
from langchain.docstore.document import Document
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

class Loader:
    """ Load documents to ElasticSearch vectordb """
    def __init__(self, cfg, es_url):
        self.cfg = cfg  # cfg 객체를 인스턴스 변수로 저장
        self.es = Elasticsearch(
            hosts=[es_url],
            http_auth=(cfg.es.user, cfg.es.pw)
        )
        logging.basicConfig(level=logging.INFO)

    def inplace_docs(self, source, folder_name, es_index, inplace=True):
        reserved_characters = ["+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\", "/"]
        doc_name = source
        if inplace:
            logging.info(f"Deleting documents with source: {doc_name} in folder: {folder_name}")
            for chr in reserved_characters:
                doc_name = doc_name.replace(f"{chr}", f"\\{chr}")
            self.es.delete_by_query(
                index=es_index,
                # body={'query': {'term': {'metadata.source.keyword': doc_name}}}
                body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"metadata.group.keyword": folder_name}},
                                {"term": {"metadata.source.keyword": doc_name}}
                            ]
                        }
                    }
                }
            )
        else:
            logging.info(f"Checking existence of documents with source: {doc_name} in folder: {folder_name}")
            for chr in reserved_characters:
                doc_name = doc_name.replace(f"{chr}", f"\\{chr}")
            res = self.es.count(
                index=es_index,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"metadata.group.keyword": folder_name}},
                                {"term": {"metadata.source.keyword": doc_name}}
                            ]
                        }
                    }
                }
                # body={'query': {'term': {'metadata.source.keyword': doc_name}}}
            )["count"]

            if res == 0:
                logging.info(f"Documents with source: {doc_name} already exist.")
                return None
            else:
                logging.info(f"Documents with source: {doc_name} in folder: {folder_name} do not exist. Proceeding to embedding...")
        return None

    def load_bulk(self, docs: List[Document], es_index, model):
        """
        Get file path to be embedded
        Get List of Documents and preprocessed as elastic cloud documents' form
        and bulk upload to elastic cloud and reindex it

        Args:
            docs: docs
        """
        requests = []
        for doc in docs:
            vector_name = "vertor"
            if model == "jhgan/ko-sroberta-multitask":
                vector_name = "vector-ko-sroberta-multitask"
            elif model == "intfloat/multilingual-e5-large-instruct":
                vector_name = "vector-multilingual-e5-large-instruct"
            vector = doc.metadata[vector_name]
            metadata = doc.metadata
            del metadata[vector_name]
            request = {
                "_op_type": "index",
                "_index": es_index,
                "text": doc.page_content,
                vector_name: vector,
                "metadata": metadata,
                "_id": metadata["uuid"],
            }
            requests.append(request)

        success, failed = helpers.bulk(self.es, requests, raise_on_error=False)
        logging.info(f"Added {success} and failed to add {failed} texts to index")
        self.es.indices.refresh(index=es_index)

        return None


    def delete_data_in_group(self, group_name: str, es_index):
        logging.info(f"Attempting to delete documents with group: {group_name}")
        try:
            self.es.delete_by_query(
                index=es_index,
                body={
                    "query": {
                        "term": {
                            "metadata.group.keyword": group_name
                        }
                    }
                },
                conflicts="proceed"
            )
            logging.info(f"Successfully deleted documents for group: {group_name}")
        except Exception as e:
            logging.error(f"Error while deleting documents for group: {group_name}. Error: {e}")
