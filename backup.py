import os
import subprocess
import boto3
from datetime import datetime
from dotenv import load_dotenv
import botocore.config

# Load environment variables
load_dotenv()

# Define backup directory
MYSQL_BACKUP_DIR = "./mysql_backup"

# Define MySQL credentials from environment variables
MYSQL_USER = os.getenv("WP_MYSQL_USER")
MYSQL_PASS = os.getenv("WP_MYSQL_PASS")
MYSQL_HOST = os.getenv("WP_MYSQL_HOST")
MYSQL_PORT = os.getenv("WP_MYSQL_PORT")
MYSQL_DB = os.getenv("WP_MYSQL_DB")

# Define S3 credentials
MYSQL_S3_FOLDER = "mysql_backup"
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

AWS_CONFIG = botocore.config.Config(read_timeout=300, connect_timeout=60)
session = boto3.Session(
    aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY
)

# Ensure backup directory exists
os.makedirs(MYSQL_BACKUP_DIR, exist_ok=True)


# Function to create MySQL backup
def create_mysql_backup(backup_file):
    try:
        print(f"[+] Creating backup: {backup_file}")
        subprocess.run(
            [
                "mysqldump",
                f"--user={MYSQL_USER}",
                f"--password={MYSQL_PASS}",
                f"--host={MYSQL_HOST}",
                f"--port={MYSQL_PORT}",
                MYSQL_DB,
                "--single-transaction",
                "--quick",
                "--lock-tables=false",
            ],
            stdout=open(backup_file, "wb"),
            check=True,
        )
        print(f"[✔] Backup created: {backup_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[✖] Error creating backup: {e}")
        return False


# Function to upload a file to S3
def upload_to_s3(file_path):
    try:
        s3 = session.resource("s3", endpoint_url=S3_ENDPOINT)
        bucket = s3.Bucket(S3_BUCKET)

        s3_key = f"{MYSQL_S3_FOLDER}/{file_path}"
        print(f"[+] Uploading {file_path} to S3: {s3_key}")
        bucket.upload_file(file_path, s3_key)
        print(f"[✔] Upload successful: {s3_key}")
    except Exception as e:
        print(f"[✖] Error uploading to S3: {e}")


def rename_or_create_file_on_s3(src_key, dest_key):
    try:
        file_exists = False
        s3 = session.client("s3", endpoint_url=S3_ENDPOINT, config=AWS_CONFIG)
        src_key = f"{MYSQL_S3_FOLDER}/{src_key}"
        dest_key = f"{MYSQL_S3_FOLDER}/{dest_key}"

        try:
            s3.head_object(Bucket=S3_BUCKET, Key=src_key)
            file_exists = True
        except Exception as e:
            print(f"Error in S3 uploading: {e}")

        if file_exists:
            # s3.delete_object(Bucket=S3_BUCKET, Key="mysql_backup/wordpress_backup_2_days_ago.gz")
            copy_source = {"Bucket": S3_BUCKET, "Key": src_key}
            s3.copy(
                CopySource=copy_source,
                Bucket=S3_BUCKET,
                Key=dest_key,
            )
            print(f"[✔] Renamed {src_key} to {dest_key} on S3")
        else:
            s3.put_object(Bucket=S3_BUCKET, Key=dest_key, Body=b"")
            print(f"[✔] Created empty file: {dest_key} on S3")

    except Exception as e:
        print(f"[✖] Error in S3 operation: {e}")


def rotate_backups():
    today = datetime.now()

    # Daily Backups (Today, Yesterday, Day-Before-Yesterday)
    if create_mysql_backup(TODAY_BACKUP):
        rename_or_create_file_on_s3(YESTERDAY_BACKUP, DAY_BEFORE_YESTERDAY_BACKUP)
        rename_or_create_file_on_s3(TODAY_BACKUP, YESTERDAY_BACKUP)
        upload_to_s3(TODAY_BACKUP)

    # Weekly Backup (Replaces every 7 days, on Sunday)
    if today.weekday() == 6:  # Sunday
        if create_mysql_backup(WEEKLY_BACKUP):
            upload_to_s3(WEEKLY_BACKUP)

    # Monthly Backup (Replaces on the 1st of the month)
    if today.day == 1:
        if create_mysql_backup(MONTHLY_BACKUP):
            upload_to_s3(MONTHLY_BACKUP)

    # 3-Month Backup (Replaces in Jan, Apr, Jul, Oct on the 1st)
    if today.month in [1, 4, 7, 10] and today.day == 1:
        if create_mysql_backup(THREE_MONTH_BACKUP):
            upload_to_s3(THREE_MONTH_BACKUP)

    # 6-Month Backup (Replaces in Apr & Jul on the 1st)
    if today.month in [4, 10] and today.day == 1:
        if create_mysql_backup(SIX_MONTH_BACKUP):
            upload_to_s3(SIX_MONTH_BACKUP)


# Run backup process
if __name__ == "__main__":
    print("[+] Starting MySQL Backup Process")
    try:
        for INDEX in [MYSQL_DB]:
            TODAY_BACKUP = f"{INDEX}_backup_today.gz"
            YESTERDAY_BACKUP = f"{INDEX}_backup_yesterday.gz"
            DAY_BEFORE_YESTERDAY_BACKUP = f"{INDEX}_backup_2_days_ago.gz"
            WEEKLY_BACKUP = f"{INDEX}_backup_weekly.gz"
            MONTHLY_BACKUP = f"{INDEX}_backup_monthly.gz"
            THREE_MONTH_BACKUP = f"{INDEX}_backup_3_months.gz"
            SIX_MONTH_BACKUP = f"{INDEX}_backup_6_months.gz"
            rotate_backups()
        # os.remove(MYSQL_BACKUP_DIR)
        print("[✔] Backup process completed")
    except Exception as e:
        print(f"Error while running backup script: {e}")
