import requests
import json
from datetime import datetime
import time

# Base URLs
BASE_URL = "https://ptaxportal.mcgm.gov.in"
WARD_LIST_URL = f"{BASE_URL}/MCGMPortal/1.0/fetchAll/wardList"
BUILDING_LIST_URL = f"{BASE_URL}/PropertyTaxWebSrv/1.0/property/fetchBldgListByWardCode"
PROPERTY_LIST_URL = f"{BASE_URL}/PropertyTaxWebSrv/1.0/property/fetchPropListByBldgId"
TAX_DETAIL_URL = f"{BASE_URL}/PropertyTaxWebSrv/1.0/property/fetchTaxDetails"

headers = {
    "Content-Type": "application/json;charset=UTF-8",
}

def build_request_header(description: str, uri_b64: str):
    return {
        "version": "1.0",
        "requestUri": uri_b64,
        "ts": datetime.utcnow().isoformat() + "Z",
        "txn": int(time.time() * 1000) % 1000000,
        "keySign": "",
        "keyIndex": "",
        "authorization": "PTIS1.0 undefined",
        "sessionRefId": "",
        "lang": "en",
        "orgId": "1",
        "oprId": "1",
        "isPortal": "Y",
        "deviceInfo": {
            "os": "windows",
            "osVersion": "windows-10",
            "deviceType": "B",
            "deviceId": "",
            "publicIp": "10.48.212.10",
            "browser": "chrome windows-10",
            "appVersion": "1.0",
            "domain": "portalserver3"
        }
    }

def post_json(url, payload):
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Request failed: {e}")
        return None

# 1. Fetch ward list
def fetch_wards():
    payload = {
        "fetchAll": {},
        "activityLog": {
            "linkId": "0",
            "clickOn": "Ward dropdown",
            "formName": "Tax Calculator",
            "discription": "Fetch Ward List"
        },
        "requestHeader": build_request_header("Fetch Ward List", "dW5kZWZpbmVk")
    }
    data = post_json(WARD_LIST_URL, payload)
    return data.get("responsePayload", {}).get("wardList", []) if data else []

# 2. Fetch building list by ward
def fetch_buildings(ward_code):
    payload = {
        "wardCode": ward_code,
        "requestHeader": build_request_header("Fetch Building List", "LzEuMC9wcm9wZXJ0eS9mZXRjaEJsZGdMaXN0QnlXYXJkQ29kZQ==")
    }
    data = post_json(BUILDING_LIST_URL, payload)
    return data.get("responsePayload", {}).get("bldgList", []) if data else []

# 3. Fetch property list by building ID
def fetch_properties(bldg_id):
    payload = {
        "bldgId": bldg_id,
        "requestHeader": build_request_header("Fetch Property List", "LzEuMC9wcm9wZXJ0eS9mZXRjaFByb3BMaXN0QnlCbGRnSWQ=")
    }
    data = post_json(PROPERTY_LIST_URL, payload)
    return data.get("responsePayload", {}).get("propList", []) if data else []

# 4. Fetch tax details for property
def fetch_tax_details(prop_id):
    payload = {
        "propertyId": prop_id,
        "requestHeader": build_request_header("Fetch Tax Details", "LzEuMC9wcm9wZXJ0eS9mZXRjaFRheERldGFpbHM=")
    }
    data = post_json(TAX_DETAIL_URL, payload)
    return data.get("responsePayload", {}) if data else {}

# 🔁 Recursive fetch example
def scrape_all_properties(limit_wards=2, limit_buildings=2, limit_props=3):
    all_data = []
    wards = fetch_wards()
    for ward in wards[:limit_wards]:
        ward_code = ward["wardCode"]
        buildings = fetch_buildings(ward_code)
        for bldg in buildings[:limit_buildings]:
            bldg_id = bldg["bldgId"]
            props = fetch_properties(bldg_id)
            for prop in props[:limit_props]:
                prop_id = prop["propertyId"]
                tax_data = fetch_tax_details(prop_id)
                all_data.append({
                    "ward": ward_code,
                    "building_id": bldg_id,
                    "property_id": prop_id,
                    "owner_name": prop.get("ownerName"),
                    "tax_data": tax_data
                })
    return all_data

# Trigger scraping of limited data for testing
scraped_sample = scrape_all_properties()
scraped_sample[:2]  # Preview first few entries
