import time
import csv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime, timedelta

# Setup driver
driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()))
driver.get("https://ptaxportal.mcgm.gov.in/CitizenPortal/#/taxCalculator")
time.sleep(5)

OUTFILE = "mcgm_tax_results.csv"
ERRORFILE = "mcgm_errors.csv"

def get_options(select_id):
    try:
        select = Select(driver.find_element(By.ID, select_id))
        return [opt for opt in select.options if opt.get_attribute("value")]
    except:
        return []

def is_disabled(select_id):
    try:
        return driver.find_element(By.ID, select_id).get_attribute("disabled") is not None
    except NoSuchElementException:
        return True

def select_by_value(select_id, value):
    try:
        Select(driver.find_element(By.ID, select_id)).select_by_value(value)
        time.sleep(1)
    except:
        pass

def get_field_value(field_id):
    try:
        return driver.find_element(By.ID, field_id).get_attribute("value").strip()
    except:
        return "N/A"

def prompt_date():
    user_input = input("📅 Enter starting 'Date of Effect' (dd/mm/yyyy): ").strip()
    try:
        return datetime.strptime(user_input, "%d/%m/%Y")
    except ValueError:
        print("❌ Invalid format. Using default: 24/06/2025")
        return datetime(2025, 6, 24)

def find_valid_date(start_date):
    date_field = driver.find_element(By.ID, "ptasdDoeffect")
    current = start_date
    while current.year >= 2020:
        date_str = current.strftime("%d/%m/%Y")
        try:
            date_field.clear()
            date_field.send_keys(date_str)
            time.sleep(1)
            if not is_disabled("ptashZoneid"):
                print(f"✅ Using valid date: {date_str}")
                return date_str
        except:
            pass
        current -= timedelta(days=30)
    raise Exception("❌ Could not find a valid date.")

def save_row(data, error=False):
    target_file = ERRORFILE if error else OUTFILE
    file_exists = os.path.exists(target_file)
    with open(target_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists or f.tell() == 0:
            writer.writeheader()
        writer.writerow(data)

# Prompt date and validate
user_start_date = prompt_date()
valid_date = find_valid_date(user_start_date)

# Begin dropdown traversal
for ward in get_options("ptashWardid"):
    select_by_value("ptashWardid", ward.get_attribute("value"))

    for zone in get_options("ptashZoneid"):
        select_by_value("ptashZoneid", zone.get_attribute("value"))

        for subzone in get_options("ptashSubzoneid"):
            select_by_value("ptashSubzoneid", subzone.get_attribute("value"))

            for occup in get_options("ptasdOccuptpid"):
                select_by_value("ptasdOccuptpid", occup.get_attribute("value"))

                for main_cat in get_options("ptasdUsrctgid"):
                    select_by_value("ptasdUsrctgid", main_cat.get_attribute("value"))

                    for sub_cat in get_options("ptasdSusubctgid"):
                        select_by_value("ptasdSusubctgid", sub_cat.get_attribute("value"))

                        floor_options = get_options("ptasdFloorid") if not is_disabled("ptasdFloorid") else [None]
                        for floor in floor_options:
                            if floor:
                                select_by_value("ptasdFloorid", floor.get_attribute("value"))
                                floor_val = floor.text
                            else:
                                floor_val = "N/A"

                            btype_options = get_options("ptasdNtbid") if not is_disabled("ptasdNtbid") else [None]
                            for btype in btype_options:
                                if btype:
                                    select_by_value("ptasdNtbid", btype.get_attribute("value"))
                                    btype_val = btype.text
                                else:
                                    btype_val = "N/A"

                                fsi_factor_val = "N/A"
                                if not is_disabled("ptasdFsifact"):
                                    fsi_factors = get_options("ptasdFsifact")
                                    if fsi_factors:
                                        select_by_value("ptasdFsifact", fsi_factors[0].get_attribute("value"))
                                        fsi_factor_val = fsi_factors[0].text

                                for meter in get_options("ptasdMetertype"):
                                    select_by_value("ptasdMetertype", meter.get_attribute("value"))

                                    try:
                                        area_field = driver.find_element(By.ID, "ptasdTcptarea")
                                        area_field.clear()
                                        area_field.send_keys("50")
                                    except:
                                        continue

                                    if not is_disabled("ptawdTaxId"):
                                        tax_options = get_options("ptawdTaxId")
                                        for tax in tax_options:
                                            select_by_value("ptawdTaxId", tax.get_attribute("value"))
                                            tax_code_val = tax.text

                                            try:
                                                driver.find_element(By.ID, "btn_submit").click()
                                                time.sleep(2)
                                                cap_value = driver.find_element(By.XPATH, '//label[text()="Capital Value"]/following-sibling::div').text.strip()
                                                total_tax = driver.find_element(By.XPATH, '//label[text()="Total Tax"]/following-sibling::div').text.strip()
                                            except:
                                                cap_value = "ERROR"
                                                total_tax = "ERROR"

                                            record = {
                                                "date": valid_date,
                                                "ward": ward.text,
                                                "zone": zone.text,
                                                "subzone": subzone.text,
                                                "occupancy": occup.text,
                                                "main_category": main_cat.text,
                                                "sub_category": sub_cat.text,
                                                "tax_code": tax_code_val,
                                                "floor": floor_val,
                                                "building_type": btype_val,
                                                "fsi_factor": fsi_factor_val,
                                                "fsi": get_field_value("ptasdFsi"),
                                                "sddr_rate": get_field_value("ptasdSddrrate"),
                                                "age_of_building": get_field_value("ptasdAge"),
                                                "metered_unmetered": meter.text,
                                                "carpet_area": "50",
                                                "capital_value": cap_value,
                                                "total_tax": total_tax
                                            }

                                            if cap_value == "ERROR" or total_tax == "ERROR":
                                                print(f"[❌ ERROR] {record}")
                                                save_row(record, error=True)
                                            else:
                                                print(f"[✅ SAVED] {record}")
                                                save_row(record)

print("✅ Scraping complete.")
driver.quit()
