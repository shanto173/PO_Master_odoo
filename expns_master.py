import os
import json
import base64
import requests
import pandas as pd
from datetime import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv

load_dotenv()

# -------------------- ENV Config --------------------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDS_JSON")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID_exp_mster", "189vmv3amNAPHNGB2O14yUId712zFN_z98sL-Bl0litU")

creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
creds = Credentials.from_service_account_info(creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# -------------------- Odoo Login --------------------
def odoo_login():
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD},
        "id": 3,
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    uid = resp.json()["result"]["uid"]
    print(f"✅ Logged in as UID {uid}")
    return uid

# -------------------- Safe Extractor --------------------
def safe_str(field, subfield=None):
    if isinstance(field, dict):
        if subfield:
            return safe_str(field.get(subfield))
        if "display_name" in field:
            return str(field.get("display_name", ""))
        return " ".join(map(str, field.values()))
    if isinstance(field, list) and len(field) == 2:
        return str(field[1]) if field[1] else ""
    if isinstance(field, (int, float)):
        return str(field)
    if field in (False, None):
        return ""
    return str(field)

# -------------------- Fetch Expense Sheets --------------------
def fetch_expense_sheets(uid, company_id):
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": uid,
        "allowed_company_ids": [1, 3, 2, 4],
        "current_company_id": company_id,
        "bin_size": True,
    }

    spec = {
        "code": {},
        "expense_line_ids": {
            "fields": {
                "date": {},
                "create_date":{},
                "product_id": {
                    "fields": {
                        "default_code": {},
                        "name": {},
                    }
                },
                "super_category_id": {"fields": {"display_name": {}}},
                "name": {},
                "department_id": {"fields": {"display_name": {}}},
                "id": {},
                "predicted_category": {"fields": {"display_name": {}}},
                "state": {},
                "total_amount": {},
                "total_amount_currency": {},
            }
        }
    }

    offset, batch_size = 0, 2000
    all_records = []

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "hr.expense.sheet",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "domain": [],
                    "specification": spec,
                    "offset": offset,
                    "limit": batch_size,
                    "context": context,
                    "count_limit": 100000,
                },
            },
            "id": 13,
        }
        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/hr.expense.sheet/web_search_read", data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()["result"]
        records = result.get("records", [])
        all_records.extend(records)
        print(f"Fetched {len(records)} (Total: {len(all_records)})")
        if len(records) < batch_size:
            break
        offset += batch_size
    return all_records

# -------------------- Flatten --------------------
def flatten_expense_sheets(records):
    flat = []
    for rec in records:
        for line in rec.get("expense_line_ids", []):
            product = line.get("product_id", {})
            category = f"[{safe_str(product.get('default_code', ''))}] {safe_str(product.get('name', ''))}" if product else ""
            flat.append({
                "Number": safe_str(rec.get("code")),
                "Expense Date": safe_str(line.get("date")),
                "Creted Date":safe_str(line.get("create_date")),
                "Category": category,
                "Super Category": safe_str(line.get("super_category_id")),
                "Description": safe_str(line.get("name")),
                "Department": safe_str(line.get("department_id")),
                "ID": safe_str(line.get("id")),
                "Predicted Category": safe_str(line.get("predicted_category")),
                "Status": safe_str(line.get("state")),
                "Total": line.get("total_amount", 0),
                "Total In Currency": line.get("total_amount_currency", 0),
            })
    return pd.DataFrame(flat)

# -------------------- Upload to GSheet --------------------
def paste_to_gsheet(df, sheet_name):
    try:
        worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
        
        if df.empty:
            print(f"⚠️ Skip: {sheet_name} is empty")
            return

        # Clear range A:K (11 columns)
        worksheet.batch_clear(["A:L"])

        # Paste the dataframe starting from A1
        set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

        # Add timestamp to L1
        local_time = datetime.now(pytz.timezone("Asia/Dhaka")).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update("M1", [[f"Last Updated: {local_time}"]])

        print(f"✅ Data pasted to {sheet_name} and timestamp updated in L1")
    except Exception as e:
        print(f"❌ Error pasting to {sheet_name}: {e}")
        raise

# -------------------- Main --------------------
if __name__ == "__main__":
    uid = odoo_login()
    # Fetch from allowed companies
    records = fetch_expense_sheets(uid, company_id=1)  # current_company_id can be any allowed company
    df = flatten_expense_sheets(records)
    paste_to_gsheet(df, "Expns_Raw_DF")
    print("✅ Data fetched and pasted")