# fetch_charges.py – updated to use real browser headers and cookie

import os
import requests
import time
from bs4 import BeautifulSoup
from pyairtable import Api
from datetime import datetime

AIRTABLE_BASE_ID = "appklERHZIa2OuacR"
AIRTABLE_TABLE_ID = "tblb0yIYr91PzghXQ"
BASE_URL = "https://www.superiorcourt.maricopa.gov/docket/CriminalCourtCases/caseInfo.asp?caseNumber="

STATIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Cookie": "cookiesEnabled=1; _ga=GA1.1.999643802.1729287917; _gcl_au=1.1.355616172.1752169833; _ga_Q0RZTDZCMF=GS2.1.s1752169833$o3$g0$t1752169841$j52$l0$h0; _ga_W7L0KQ6EGZ=GS2.1.s1752169833$o3$g0$t1752169841$j52$l0$h0; ASPSESSIONIDSWDQTDSB=KOCAODJALIKPEMLPCMIFECKK; _ga_Y8Q8DRN6NX=GS2.1.s1752773060$o297$g1$t1752775112$j51$l0$h0",
    "Referer": "https://www.superiorcourt.maricopa.gov/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1"
}

def connect_to_airtable():
    api_key = os.environ["KEY"]
    api = Api(api_key)
    return api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

def get_case_links(table):
    records = table.all()
    return [(rec['id'], rec['fields'].get('Case #')) for rec in records if rec['fields'].get('Case #')]

def extract_charge_with_priority(soup):
    charges_section = soup.find("div", id="tblDocket12")
    if not charges_section:
        return None

    rows = charges_section.find_all("div", class_="row g-0")
    first_charge = None
    for row in rows:
        divs = row.find_all("div")
        for i in range(len(divs)):
            if "Description" in divs[i].get_text(strip=True):
                description = divs[i + 1].get_text(strip=True)
                if not first_charge:
                    first_charge = description
                if "MURDER" in description.upper():
                    return description
    return first_charge

def extract_today_event(soup):
    today_str = datetime.now().strftime("%-m/%-d/%Y").replace("/0", "/")
    calendar_section = soup.find("div", id="tblForms4")
    if not calendar_section:
        return None

    rows = calendar_section.find_all("div", class_="row g-0")
    for row in rows:
        cols = row.find_all("div")
        if len(cols) >= 6 and cols[1].get_text(strip=True) == today_str:
            return cols[5].get_text(strip=True)
    return None

def log_and_check_page_message(soup, case_number):
    text = soup.get_text(separator="\n", strip=True).lower()
    known_fail_phrases = [
        "server is busy", "could not be found", "error has occurred",
        "unavailable", "temporarily unavailable", "try again later"
    ]
    print(f"[{case_number}] Page snippet: {text[:300].replace(chr(10), ' ')}")

    for phrase in known_fail_phrases:
        if phrase in text:
            print(f"[{case_number}] ❌ Skipping due to error message → '{phrase}'")
            return True
    return False

def update_airtable_with_details(table, records):
    session = requests.Session()
    session.headers.update(STATIC_HEADERS)

    for record_id, case_number in records:
        full_url = BASE_URL + case_number

        try:
            response = session.get(full_url)
            print(f"[{case_number}] 🔍 Status: {response.status_code}")
            print(f"[{case_number}] 🔍 Response headers: {response.headers}")

            if response.status_code != 200:
                print(f"[{case_number}] ❌ Failed: status code {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            if log_and_check_page_message(soup, case_number):
                with open(f"error_page_{case_number}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                continue

            charge = extract_charge_with_priority(soup)
            today_event = extract_today_event(soup)

            fields_to_update = {}
            if charge:
                fields_to_update["Crime"] = charge
            if today_event:
                fields_to_update["Case Number Links"] = today_event

            if fields_to_update:
                print(f"[{case_number}] ✅ Updating Airtable: {fields_to_update}")
                table.update(record_id, fields_to_update)
            else:
                print(f"[{case_number}] No updates.")

        except requests.exceptions.RequestException as e:
            print(f"[{case_number}] ❌ Request error: {e}")

        time.sleep(4)  # slight, fixed delay between requests

if __name__ == "__main__":
    table = connect_to_airtable()
    records = get_case_links(table)
    update_airtable_with_details(table, records)
