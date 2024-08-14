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
    def __init__(self, cfg):
        self.cfg = cfg  # cfg 객체를 인스턴스 변수로 저장
        self.es = Elasticsearch(
            hosts=[cfg.es.url],
            http_auth=(cfg.es.user, cfg.es.pw)
        )
        logging.basicConfig(level=logging.INFO)

    def inplace_docs(self, source, inplace=True):
        doc_name: str = source
        if inplace:
            reserved_character = ["+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\", "/"]
            logging.info(f"replacing {doc_name}")
            for chr in reserved_character:
                doc_name = doc_name.replace(f"{chr}", f"\\{chr}")
            self.es.delete_by_query(
                index=self.cfg.es.index_name,
                body={'query': {'term': {'metadata.source.keyword': doc_name}}}
            )
        else:
            reserved_character = ["+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\", "/"]
            logging.info(f"replacing {doc_name}")
            for chr in reserved_character:
                doc_name = doc_name.replace(f"{chr}", f"\\{chr}")
            res = self.es.count(
                index=self.cfg.es.index_name,
                body={'query': {'term': {'metadata.source.keyword': doc_name}}}
            )["count"]

            if res == 0:
                logging.info(f"{doc_name} already exists")
                return None
            else:
                logging.info(f"{doc_name} doesn't exist. embedding...")
                pass
        return None

    def load_bulk(self, docs: List[Document]):
        """
        Get file path to be embedded
        Get List of Documents and preprocessed as elastic cloud documents' form
        and bulk upload to elastic cloud and reindex it

        Args:
            docs: docs
        """
        requests = []
        for doc in docs:
            vector = doc.metadata["vector"]
            metadata = doc.metadata
            del metadata["vector"]
            request = {
                "_op_type": "index",
                "_index": self.cfg.es.index_name,
                "text": doc.page_content,
                "vector": vector,
                "metadata": metadata,
                "_id": metadata["uuid"],
            }
            requests.append(request)

        success, failed = helpers.bulk(self.es, requests, raise_on_error=False)
        logging.info(f"Added {success} and failed to add {failed} texts to index")
        self.es.indices.refresh(index=self.cfg.es.index_name)

        return None
