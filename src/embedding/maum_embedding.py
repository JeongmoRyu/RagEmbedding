import requests
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class MaumEmebedding:
    def __init__(self, cfg, model):
        self.cfg = cfg
        # self.model = model  
        if isinstance(model, str):
            self.model = model
        else:
            self.model = getattr(model, 'model', "text-embedding-ada-002")


    def embed_query(self, query):
        url = self.cfg.embeddingserver.host + '/embed'
        logging.info(f"Response model: {self.model}")

        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "text": query,
            "model": self.model
        }
        for i in range(10):
            try:
                response = requests.post(url, headers=headers, json=data)
                # logging.info(f"Response content: {response.text[:100]}")
                response_data = response.json()
                response.raise_for_status()
                # response_data = response.json()
                # return response_data
                try:
                    response_data = response.json()
                    return response_data['result'][0]
                except ValueError as e:
                    logging.error(f"Error parsing JSON: {e}")
                    return None
                
            except requests.exceptions.RequestException as e:
                wait_time = 2 ** i
                logging.error(f"ERROR {e} : retrying after {wait_time} seconds (exponential backoff)")
                time.sleep(wait_time)

        raise Exception("Request failed after {} retries".format(10))
