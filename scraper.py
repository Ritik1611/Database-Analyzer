import requests
import itertools
import csv
import time

# Endpoints and headers
BASE = "https://ptaxportal.mcgm.gov.in/MCGMPortal/1.0"
CALC = "https://ptaxportal.mcgm.gov.in/MCGMPortal/1.0/calculate"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
    "Referer": "https://ptaxportal.mcgm.gov.in/CitizenPortal/#/taxCalculator"
}

def post(endpoint, payload=None):
    url = f"{BASE}/{endpoint}"
    r = requests.post(url, json=payload or {}, headers=HEADERS)
    r.raise_for_status()
    return r.json().get("data", [])

def fetch_calc(payload):
    r = requests.post(CALC, json=payload, headers=HEADERS)
    r.raise_for_status()
    return r.json().get("data", {})

def main():
    wards = post("fetchAll/wardList")
    occupancy = post("fetchOccupanyType")
    sddr = post("masters/propertyDataEntry/fetchUserCategorySddr")

    results = []

    for ward in wards[:3]:  # limit for test
        zones = post("fetchZone", {"ward": ward["code"]})
        for zone in zones[:2]:
            subzones = post("fetchSubZone", {"ward": ward["code"], "zone": zone["code"]})
            for sub in subzones[:1]:
                for occ in occupancy:
                    for um in sddr:
                        payload = {
                            "ward": ward["code"], "zone": zone["code"], "subZone": sub["code"],
                            "occupancyType": occ["code"], "dateOfEffect": "23/06/2025",
                            "userMainCategory": um["code"]
                        }

                        floor = post("masters/propertyDataEntry/fetchFloorFactor", payload)[0]
                        usub = post("masters/propertyDataEntry/fetchUserSubCategory", payload)[0]
                        payload.update({"userSubCategory": usub["code"]})

                        taxcode = post("masters/propertyDataEntry/fetchTaxCode", payload)[0]
                        payload.update({
                            "floor": floor["code"],
                            "carpetArea": 500,
                            "fsiFactor": "Admissible",
                            "fsi": 1.33,
                            "meteredUnmetered": "Metered",
                            "taxCode": taxcode["code"]
                        })

                        data = fetch_calc(payload)
                        results.append({
                            "ward": ward["code"],
                            "zone": zone["code"],
                            "subZone": sub["code"],
                            "occupancyType": occ["code"],
                            "userMainCat": um["code"],
                            "userSubCat": usub["code"],
                            "taxCode": taxcode["code"],
                            "capitalValue": data.get("capitalValue"),
                            "totalTax": data.get("totalTax")
                        })
                        print("🧾", results[-1])
                        time.sleep(0.5)

    # Save CSV
    with open("mumbai_tax.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("✅ Saved mumbai_tax.csv")

if __name__ == "__main__":
    main()
