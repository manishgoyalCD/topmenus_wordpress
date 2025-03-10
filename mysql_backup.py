# Filename: mongo_backup.py
# Description: This script takes a dump of a MySQL database from a WordPress site and uploads it to S3.
#
# Response Types:
# - Logs informational and error messages during the backup and upload process.
#
# Dependencies:
# - boto3
# - python-dotenv

import os
import sys
import subprocess
import boto3
from datetime import datetime
# from constant import *
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger import ErrorLogger
from dotenv import load_dotenv
load_dotenv(override=True)
from os import getenv

logget = ErrorLogger(log_file_name='mysql-backup', log_to_terminal=True)

# S3 bucket details

s3_folder = 'mysql_backup'

# Backup file details
backup_dir = './'
backup_filename = f"{getenv('WP_MYSQL_DB')}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.gz"
backup_filepath = os.path.join(backup_dir, backup_filename)

def create_mysql_backup():
    try:
        logget.info(msg="[+] Creating Backup")
        # Create the backup directory if it doesn't exist
        os.makedirs(backup_dir, exist_ok=True)

        # Run the mysqldump command to create a backup
        subprocess.check_call([
            'mysqldump',
            f'--user={getenv("WP_MYSQL_USER")}',
            f'--password={getenv("WP_MYSQL_PASS")}',
            f'--host={getenv("WP_MYSQL_HOST")}',
            f'--port={getenv("WP_MYSQL_PORT")}',
            getenv("WP_MYSQL_DB"),
            '--result-file=' + backup_filepath,
            '--single-transaction',
            '--quick',
            '--lock-tables=false'
        ])
        logget.info(msg=f"Backup created at {backup_filepath}")
        # upload_to_s3()
        # os.remove(backup_filepath)
    except subprocess.CalledProcessError as e:
        logget.exception(msg=f"[-] Error creating backup: {e}")
        raise

def upload_to_s3():
    try:
        logget.info(msg="[+] Uploading file")
        session = boto3.Session(
        aws_access_key_id=getenv('S3_ACCESS_KEY'),
        aws_secret_access_key=getenv('S3_SECRET_KEY'),
        )
        s3_resource = session.resource('s3', endpoint_url = getenv('S3_ENDPOINT'))
        bucket = s3_resource.Bucket(getenv('S3_BUCKET'))

        s3_key = '{}/{}'.format(s3_folder,backup_filename)
        logget.info(msg=s3_key)
        bucket.upload_file(backup_filepath, s3_key)
        logget.info(msg="[+] Uploaded successfully")
    except Exception as e:
        logget.exception(msg="[-] Error while uploading file")


if __name__ == "__main__":
    start = datetime.now()
    logget.info(msg=f'[+] Started at :' + str(start))
    create_mysql_backup()
    end_time = datetime.now()-start
    logget.info(msg=f'[++] Time taken : ' + str(end_time))
