import pandas as pd
import re
from datetime import datetime

def extract_date(filename, pattern):
    match = re.search(pattern, filename)
    if match:
        raw_date = match.group(1)
        return datetime.strptime(raw_date, "%Y%m%d")
    return None

def process_files(file_list):
    for file in file_list:
        if file.startswith("CUST_MSTR_"):
            date = extract_date(file, r"CUST_MSTR_(\d{8})")
            df = pd.read_csv(file)
            df["Date"] = date.date()
            truncate_and_load(df, "CUST_MSTR")

        elif file.startswith("master_child_export-"):
            date = extract_date(file, r"master_child_export-(\d{8})")
            df = pd.read_csv(file)
            df["Date"] = date.date()
            df["DateKey"] = date.strftime("%Y%m%d")
            truncate_and_load(df, "master_child")

        elif file.startswith("H_ECOM_ORDER"):
            df = pd.read_csv(file)
            truncate_and_load(df, "H_ECOM_Orders")

def truncate_and_load(df, table_name):
    # Truncate the table and insert data
    # Example: Using SQLAlchemy or any DB API
    pass
