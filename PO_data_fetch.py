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
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "10fn6IXNlXf0s6pGyUKSbAi3Gn3NfCrGEoF6zJWFbCvQ")

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

# -------------------- Fetch Purchase Orders --------------------
def fetch_purchase_orders(uid, company_id):
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": uid,
        "allowed_company_ids": [3, 1],
        "quotation_only": True,
        "current_company_id": company_id,
        "bin_size": True,
    }

    spec = {
        "order_line": {
            "fields": {
                "company_id": {"fields": {"display_name": {}}},          # Order Lines/Company
                "create_date": {},                                       # Order Lines/Created on
                "exp_consum_date": {},                                   # Order Lines/Consumption Date
                "date_approve":{},
                "order_id": {"fields": {"display_name": {}}},            # Order Lines/Order Reference
                "po_type": {"fields": {"display_name": {}}},             # Order Lines/PO Type (assumed relational)
                "itemtypes": {"fields": {"display_name": {}}},           # Order Lines/Item Type (assumed relational)
                "currency_id": {"fields": {"display_name": {}}},         # Order Lines/Currency
                "item_category": {"fields": {"display_name": {}}},       # Order Lines/Item Category (assumed relational)
                "name": {},                                              # Order Lines/Description
                "partner_id": {"fields": {"display_name": {}}},          # Order Lines/Partner
                "incoterm_id": {"fields": {"display_name": {}}},         # Order Lines/Inco Term
                "payment_term_id": {"fields": {"display_name": {}}},     # Order Lines/Payment Term
                "shipment_mode": {"fields": {"display_name": {}}},       # Order Lines/Shipment Mode 
                "product_uom_qty": {},                                   # Order Lines/Total Quantity
                "price_subtotal": {},                                    # Order Lines/Subtotal
                "state":{"fields": {"display_name": {}}}
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
                "model": "purchase.order",
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
        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/purchase.order/web_search_read", data=json.dumps(payload))
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
def flatten_purchase_orders(records):
    flat = []
    for rec in records:
        for line in rec.get("order_line", []):
            flat.append({
                "Company": safe_str(line.get("company_id")),
                "Created on": safe_str(line.get("create_date")),
                "Consumption Date": safe_str(line.get("exp_consum_date")),
                "PO Approved Date": safe_str(line.get("date_approve")),
                "Order Reference": safe_str(line.get("order_id")),
                "PO Type": safe_str(line.get("po_type")),
                "Item Type": safe_str(line.get("itemtypes")),
                "Currency": safe_str(line.get("currency_id")),
                "Item Category": safe_str(line.get("item_category")),
                "Description": safe_str(line.get("name")),
                "Partner": safe_str(line.get("partner_id")),
                "Inco Term": safe_str(line.get("incoterm_id")),
                "Payment Term": safe_str(line.get("payment_term_id")),
                "Shipment Mode": safe_str(line.get("shipment_mode")),
                "Total Quantity": line.get("product_uom_qty", 0),
                "Subtotal": line.get("price_subtotal", 0),
                "Status": safe_str(line.get("state")),
                
            })
    return pd.DataFrame(flat)

# -------------------- Upload to GSheet --------------------
def paste_to_gsheet(df, sheet_name):
    try:
        worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
        
        if df.empty:
            print(f"⚠️ Skip: {sheet_name} is empty")
            return

        # Clear range A:N (14 columns)
        worksheet.batch_clear(["A:Q"])

        # Paste the dataframe starting from A1
        set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

        # Add timestamp to O1
        local_time = datetime.now(pytz.timezone("Asia/Dhaka")).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update("R1", [[f"Last Updated: {local_time}"]])

        print(f"✅ Data pasted to {sheet_name} and timestamp updated in O1")
    except Exception as e:
        print(f"❌ Error pasting to {sheet_name}: {e}")
        raise

# -------------------- Main --------------------
if __name__ == "__main__":
    uid = odoo_login()
    # Only one fetch is needed (fetches from allowed companies)
    records = fetch_purchase_orders(uid, company_id=3)  # current_company_id can be any allowed company
    df = flatten_purchase_orders(records)
    paste_to_gsheet(df, "Raw_data")
    print("✅ Data fetched and pasted")