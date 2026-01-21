import os
import boto3

from pay_ccprov_clean import clean_ccprov_and_ccstaff
from pay_ccprov_upload import upload_to_postgres

s3 = boto3.client("s3")

BUCKET = os.environ["S3_BUCKET"]
PREFIX = os.getenv("S3_PREFIX", "")

WANTS = {
    "ccprov1": lambda k: os.path.basename(k).startswith("ccprov1_") and k.lower().endswith(".xls"),
    "ccprov2": lambda k: os.path.basename(k).startswith("ccprov2_") and k.lower().endswith(".xls"),
    "ccstaff": lambda k: os.path.basename(k).startswith("ccstaff_") and k.lower().endswith(".xls"),
    "labor":   lambda k: os.path.basename(k).startswith("Labor_Summary_by_Employee_Retool_Annual_Export_") and k.lower().endswith(".xlsx"),
}

def _list_objects(bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix 
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            yield obj  

def _download(bucket: str, key: str) -> str:
    local = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, local)
    return local

def handler(event, context):
    # Find newest key for each required file bucket
    newest = {name: None for name in WANTS.keys()} 

    for obj in _list_objects(BUCKET, PREFIX):
        key = obj["Key"]
        for name, predicate in WANTS.items():
            if predicate(key):
                cur = newest[name]
                if cur is None or obj["LastModified"] > cur["LastModified"]:
                    newest[name] = obj

    missing = [name for name, obj in newest.items() if obj is None]
    if missing:
        raise RuntimeError(f"Missing required files in s3://{BUCKET}/{PREFIX or ''}: {missing}")

    # Download
    ccprov1_local = _download(BUCKET, newest["ccprov1"]["Key"])
    ccprov2_local = _download(BUCKET, newest["ccprov2"]["Key"])
    ccstaff_local = _download(BUCKET, newest["ccstaff"]["Key"])
    labor_local   = _download(BUCKET, newest["labor"]["Key"])

    ccprov_csv, ccstaff_csv = clean_ccprov_and_ccstaff(
        ccprov_paths=[ccprov1_local, ccprov2_local],
        ccstaff_paths=[ccstaff_local],
        out_dir="/tmp",
    )

    upload_to_postgres(
        ccprov_csv_path=ccprov_csv,
        ccstaff_csv_path=ccstaff_csv,
        labor_csv_path=labor_local,
    )

    return {
        "ok": True,
        "picked": {k: v["Key"] for k, v in newest.items()},
    }
