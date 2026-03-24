#!/usr/bin/env python3
"""
PostgreSQL Automated Backup Script
Backs up databases to AWS S3 with retention policy
Author: [Your Name] | DBA Portfolio
"""

import subprocess
import boto3
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "myapp")
DB_USER     = os.getenv("DB_USER", "postgres")
S3_BUCKET   = os.getenv("S3_BUCKET", "my-db-backups")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))

def create_backup() -> str:
    """Run pg_dump and return the backup file path."""
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"/tmp/{DB_NAME}_{timestamp}.sql.gz"

    cmd = [
        "pg_dump",
        f"--host={DB_HOST}",
        f"--port={DB_PORT}",
        f"--username={DB_USER}",
        f"--dbname={DB_NAME}",
        "--format=custom",
        "--compress=9",
        f"--file={backup_file}"
    ]

    logger.info(f"Starting backup of {DB_NAME}...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr}")

    logger.info(f"Backup created: {backup_file}")
    return backup_file

def upload_to_s3(file_path: str) -> str:
    """Upload backup file to S3 and return the S3 key."""
    s3     = boto3.client("s3")
    s3_key = f"backups/{DB_NAME}/{os.path.basename(file_path)}"

    logger.info(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
    s3.upload_file(file_path, S3_BUCKET, s3_key)
    logger.info("Upload complete.")
    return s3_key

def cleanup_old_backups():
    """Delete S3 backups older than RETENTION_DAYS."""
    s3         = boto3.client("s3")
    cutoff     = datetime.now() - timedelta(days=RETENTION_DAYS)
    prefix     = f"backups/{DB_NAME}/"
    paginator  = s3.get_paginator("list_objects_v2")

    deleted = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"].replace(tzinfo=None) < cutoff:
                s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
                logger.info(f"Deleted old backup: {obj['Key']}")
                deleted += 1

    logger.info(f"Cleanup complete. Removed {deleted} old backup(s).")

if __name__ == "__main__":
    try:
        backup_file = create_backup()
        upload_to_s3(backup_file)
        cleanup_old_backups()
        os.remove(backup_file)          # remove local temp file
        logger.info("✅ Backup process completed successfully.")
    except Exception as e:
        logger.error(f"❌ Backup failed: {e}")
        raise
