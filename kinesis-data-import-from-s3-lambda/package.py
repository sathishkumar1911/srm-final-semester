import zipfile
import os
import shutil
from logger import console_logger
from datetime import datetime

# Initialize the logger for the package.py file
logger = console_logger.config_logger(__name__)

files_to_zip = ["main.py", "logger"]

zip_file_dir = "target/"
zip_file_name = "kinesis-data-import-form-s3-" + datetime.today().strftime('%Y%m%d%H%M%S') + ".zip"


def zip_files(zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'w') as zf:
        for file_to_zip in files_to_zip:
            if os.path.isfile(file_to_zip):
                logger.info(f"Adding the file {file_to_zip} to the zip file {zip_file_path}")
                zf.write(file_to_zip)
            elif os.path.isdir(file_to_zip):
                for root, _, files in os.walk(file_to_zip):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        if file_path.endswith(".py"):
                            logger.info(f"Adding the file {file_to_zip} to the zip file {zip_file_path}")
                            zf.write(file_path)
                        else:
                            logger.info(f"Skipping the file {file_path} from zipping")
            else:
                logger.error(f"The file {file_to_zip} does not exist")

def delete_existing_target_files(file_dir):
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    files = os.listdir(file_dir)

    for file_name in files:
        file_path = os.path.join(file_dir, file_name)
        if os.path.isfile(file_path):
            logger.info("Deleting the file %s", file_path)
            os.remove(file_path)
        elif os.path.isdir(file_path):
            logger.info("Deleting the folder %s", file_path)
            shutil.rmtree(file_path)


delete_existing_target_files(zip_file_dir)
zip_files(zip_file_dir + zip_file_name)


