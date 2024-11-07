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
        logging.info(f"Accessing local directory: `{local_path}` and prefix : `{prefix}`")
        output = {ext: [] for ext in file_extensions}
        # Convert to absolute path
        absolute_path = os.path.abspath(local_path)
        logging.info(f"Absolute path: `{absolute_path}`")

        # Check if the directory exists
        if not os.path.isdir(absolute_path):
            logging.error(f"Directory does not exist: {absolute_path}")
            return output

        # Check read permissions
        if os.access(absolute_path, os.R_OK):
            logging.info(f"Read permission is available for `{absolute_path}`")
        else:
            logging.error(f"Read permission is not available for `{absolute_path}`")

        # List directory contents
        try:
            contents = os.listdir(absolute_path)
            logging.info(f"Contents of `{absolute_path}`: {contents}")
        except Exception as e:
            logging.error(f"Error listing directory contents: {e}")
            return output

        # Walk through the directory
        for root, dirs, files in os.walk(absolute_path):
            logging.info(f"Checking directory: {root}")

            for file in files:
                logging.info(f"Found file: {file}")
                if prefix and not file.startswith(prefix):
                    continue
                for ext in file_extensions:
                    if file.lower().endswith(ext.lower()): 
                        output[ext].append(os.path.join(root, file))

        logging.info(f"Files found: {output}")
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
            logging.error(f"Failed to copy file: {e}")
            raise e
        return None
