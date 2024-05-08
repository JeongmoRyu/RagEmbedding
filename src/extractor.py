import os
import traceback
from typing import Dict, List
from omegaconf import OmegaConf
import logging

class Extractor:
    """ Extract File from Local Directory """
    def __init__(self, cfg):
        # if isinstance(cfg, str):
        #     cfg = OmegaConf.load(cfg)
        # if isinstance(cfg, OmegaConf):
        #     self.cfg = cfg 
        # else:
        #     raise TypeError("Expected a configuration object or path")
        # self.local_path = self.cfg.path
        # self.prefix = self.cfg.prefix
        # if isinstance(cfg, OmegaConf):
        #     print('Received cfg as OmegaConf object')
        #     # `cfg`가 `OmegaConf` 객체이면, `cfg`를 직접 사용
        #     self.cfg = cfg
        # elif isinstance(cfg, dict):
        #     print('Received cfg as dictionary object, converting to OmegaConf')
        #     # `cfg`가 `dict` 객체이면 `OmegaConf`로 변환
        #     self.cfg = OmegaConf.create(cfg)
        # else:
        #     raise TypeError("Expected a configuration object or dictionary")
        
        # Extract `path` and `prefix` from `self.cfg`
        self.local_path = cfg.local.path
        self.prefix = cfg.local.prefix
        # self.cfg = OmegaConf.load(cfg)["local"]
        logging.basicConfig(level=logging.INFO)

    def get_file_list_from_local(
            self,
            local_path,
            prefix,
            file_extensions: List[str]
            ) -> Dict:
        """
        Get file path to be embedded

        Args:
            local_path: local directory path
            file_extensions: list of extensions that you want to embed e.g) ["xlsx", "pdf"]

        Returns:
            Dict: file lists of extensions
        """
        logging.info(f"Accessing local directory: `{local_path}` and prefix : `{prefix}")
        output = {ext: [] for ext in file_extensions}
        
        for root, dirs, files in os.walk(local_path):
            for file in files:
                for ext in file_extensions:
                    if file.endswith(ext):
                        output[ext].append(os.path.join(root, file))
        
        return output

    def download_file(self, local_path, local_file, destination_path) -> None:
        """
        Copy file to destination path

        Args:
            local_path: local directory path
            local_file: local file path including prefix
            destination_path: destination path to copy file
        """
        try:
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            with open(local_file, "rb") as src, open(destination_path, "wb") as dst:
                logging.info(f"Copying {local_file} to {destination_path}")
                dst.write(src.read())
        except BaseException as e:
            print(traceback.format_exc())
            raise e
        return None
