import os
import re
import urllib.parse
import boto3
from botocore.exceptions import ClientError

from pay_ccprov_clean import clean_ccprov_and_ccstaff
from pay_ccprov_upload import upload_to_postgres

s3 = boto3.client("s3")

DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")  # finds YYYY-MM-DD anywhere in the name

# Templates for the 4 required files
REQUIRED_TEMPLATES = {
    "ccprov1": "ccprov1_{date}.xls",
    "ccprov2": "ccprov2_{date}.xls",
    "ccstaff": "ccstaff_{date}.xls",
    "labor": "Labor_Summary_by_Employee_Retool_Annual_Export_{date}.csv",
}

def _extract_date_from_key(key: str) -> str:
    fname = key.split("/")[-1]
    m = DATE_RE.search(fname)
    if not m:
        raise ValueError(f"No YYYY-MM-DD date found in filename: {fname}")
    return m.group(1)

def _s3_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def _download(bucket: str, key: str) -> str:
    local_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, local_path)
    return local_path

def handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    # 1) Determine date for "that day" from the filename
    date = _extract_date_from_key(key)

    # 2) Build the 4 expected keys (in bucket root)
    expected = {name: tmpl.format(date=date) for name, tmpl in REQUIRED_TEMPLATES.items()}

    # 3) Idempotency / avoid double-runs
    done_key = f"_DONE_{date}.txt"
    if _s3_exists(bucket, done_key):
        print(f"Already processed {date} (found {done_key}). Exiting.")
        return {"ok": True, "ready": True, "already_done": True, "date": date}

    # 4) Check if all required files exist yet
    missing = [k for k in expected.values() if not _s3_exists(bucket, k)]
    if missing:
        print(f"Not ready for {date}. Missing: {missing}")
        return {"ok": True, "ready": False, "missing": missing, "date": date}

    print(f"All 4 files present for {date}. Running pipeline...")

    # 5) Download all 4
    ccprov1_local = _download(bucket, expected["ccprov1"])
    ccprov2_local = _download(bucket, expected["ccprov2"])
    ccstaff_local = _download(bucket, expected["ccstaff"])
    labor_local   = _download(bucket, expected["labor"])

    # 6) Clean: produces two cleaned CSVs in /tmp
    ccprov_csv, ccstaff_csv = clean_ccprov_and_ccstaff(
        ccprov_paths=[ccprov1_local, ccprov2_local],
        ccstaff_paths=[ccstaff_local],
        out_dir="/tmp",
    )

    # 7) Upload/load
    upload_to_postgres(
        ccprov_csv_path=ccprov_csv,
        ccstaff_csv_path=ccstaff_csv,
        labor_csv_path=labor_local,
    )

    # 8) Write DONE marker so re-triggers don't re-run the load
    s3.put_object(
        Bucket=bucket,
        Key=done_key,
        Body=f"Processed date {date}\n".encode("utf-8"),
        ContentType="text/plain",
    )

    return {"ok": True, "ready": True, "date": date}
