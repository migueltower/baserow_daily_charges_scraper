import os
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# --- NEW: imports required for SSL fallback ---
import urllib3
from requests.exceptions import SSLError

# Disable warnings when retrying with verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------
# CONFIGURATION
# ---------------------------
BASEROW_API = "https://api.baserow.io/api"
TABLE_ID = os.getenv("BASEROW_TABLE_ID", "709546")
TOKEN = os.environ["BASEROW_TOKEN"]

BASE_URL = (
    "https://www.superiorcourt.maricopa.gov/docket/CriminalCourtCases/caseInfo.asp?caseNumber="
)

STATIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Referer": "https://www.superiorcourt.maricopa.gov/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1",
}


# ---------------------------
# BASEROW HELPERS
# ---------------------------
def baserow_session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {TOKEN}"})
    return s


def list_rows(session, page_size=200):
    url = f"{BASEROW_API}/database/rows/table/{TABLE_ID}/?user_field_names=true&size={page_size}"
    while url:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for row in data.get("results", []):
            yield row["id"], row.get("Case #"), row
        url = data.get("next")


def update_row(session, row_id, fields):
    if not fields:
        return
    url = f"{BASEROW_API}/database/rows/table/{TABLE_ID}/{row_id}/?user_field_names=true"
    resp = session.patch(url, json=fields, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ---------------------------
# SCRAPER HELPERS (unchanged)
# ---------------------------
def extract_charge_with_priority(soup):
    charges_section = soup.find("div", id="tblDocket12")
    if not charges_section:
        return None

    rows = charges_section.find_all("div", class_="row g-0")
    first_charge = None

    for row in rows:
        divs = row.find_all("div")
        for i in range(len(divs)):
            text = divs[i].get_text(strip=True)
            if not text:
                continue
            if "Description" in text and i + 1 < len(divs):
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


def page_has_error_message(soup, case_number):
    text = soup.get_text(separator="\n", strip=True).lower()
    known_fail_phrases = [
        "server is busy",
        "could not be found",
        "error has occurred",
        "unavailable",
        "temporarily unavailable",
        "try again later",
    ]

    snippet = text[:300].replace("\n", " ")
    print(f"[{case_number}] Page snippet: {snippet}")

    for phrase in known_fail_phrases:
        if phrase in text:
            print(f"[{case_number}] ❌ Skipping due to error message → '{phrase}'")
            return True

    return False


# ---------------------------
# CORE LOGIC (ONLY SSL FIX ADDED)
# ---------------------------
def process_cases():
    api = baserow_session()
    http = requests.Session()
    http.headers.update(STATIC_HEADERS)

    updated = 0
    skipped = 0
    failed = 0

    for row_id, case_number, row in list_rows(api):
        case_number = str(case_number or "").strip()
        if not case_number:
            continue

        full_url = BASE_URL + case_number

        try:
            # ---- SSL FIX: TRY NORMAL, THEN FALLBACK WITHOUT VERIFY ----
            try:
                response = http.get(full_url, timeout=60)
            except SSLError as e:
                print(f"[{case_number}] ⚠️ SSL error: {e}")
                print(f"[{case_number}] ⚠️ Retrying with verify=False")
                response = http.get(full_url, timeout=60, verify=False)

            print(f"[{case_number}] 🔍 Status: {response.status_code}")
            if response.status_code != 200:
                skipped += 1
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            if page_has_error_message(soup, case_number):
                with open(f"error_page_{case_number}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                skipped += 1
                continue

            charge = extract_charge_with_priority(soup)
            today_event = extract_today_event(soup)

            fields_to_update = {}
            if charge:
                fields_to_update["Crime"] = charge
            if today_event:
                fields_to_update["Case Number Links"] = today_event

            if fields_to_update:
                print(f"[{case_number}] ✅ Updating {row_id}: {fields_to_update}")
                update_row(api, row_id, fields_to_update)
                updated += 1
            else:
                skipped += 1

        except requests.RequestException as e:
            print(f"[{case_number}] ❌ Request error: {e}")
            failed += 1

        time.sleep(4)

    print(f"✅ Done. Updated={updated}, Skipped={skipped}, Failed={failed}")


# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    process_cases()
