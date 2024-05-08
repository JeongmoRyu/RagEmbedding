import boto3
import traceback
from typing import Dict, List
from omegaconf import OmegaConf
import logging

### TODO: add loggging to extractor
class Extractor:
    """ Extract File from S3 """
    def __init__(self, cfg):
        self.client = boto3.client('s3')
        self.cfg = OmegaConf.load(cfg)["s3"]

        ### logging status
        logging.basicConfig(level=logging.INFO)

    def get_file_list_from_s3(
            self,
            bucket,
            prefix,
            file_extensions:List[str],
            ) -> Dict:
        """
        Get file path to be embedded

        Args:
            bucket: s3 bucket
            prefix: s3 prefix
            file_extensions: list of extensions that you want to embed e.g) ["xlsx", "pdf"]

        Returns:
            Dict: file lists of extensions

        """
        logging.info(f"Accessing : bucket `{bucket}` and prefix `{prefix}`")
        obj_list = self.client.list_objects_v2(Bucket=bucket,
                            Prefix=prefix,
                            )
        
        try:
            contents_list = obj_list["Contents"]
            contents_list = list(dict.fromkeys(contents_list)) ### remove duplicates

        except Exception:
            print(f"no files in {bucket}/{prefix}")

        output = {}
        for extension in file_extensions:
            output[extension] = [i["Key"] for i in contents_list if i["Key"].endswith(extension)]
        
        return output
    
    def download_file(self,
                      bucket,
                      s3_file,
                      local_path
                      ) -> None:
        """
        Download file's to local path

        Args:
            bucket: s3 bucket
            s3_file: s3 file path including prefix
            local_path: local path to download file

        """
        
        try:
            with open(local_path, "wb+") as data:
                logging.info(f"downloading {s3_file} as {local_path}")
                self.client.download_fileobj(
                    bucket, s3_file, data
                )
        except BaseException as e:
            print(traceback.format_exc())
            raise e
        return None