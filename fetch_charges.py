# fetch_charges.py — Baserow version
# Reads rows from your Baserow "MC Daily" table (ID 709546),
# scrapes the court page for each "Case #",
# and writes back fields like "Crime" and "Time".
#
# Environment variables expected:
#   BASEROW_TOKEN   -> your Baserow database token (required)
#   BASEROW_TABLE_ID-> defaults to "709546" if not set

import os
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

BASEROW_API = "https://api.baserow.io/api"
TABLE_ID = os.getenv("BASEROW_TABLE_ID", "709546")
TOKEN = os.environ["BASEROW_TOKEN"]  # will raise KeyError if missing

BASE_URL = "https://www.superiorcourt.maricopa.gov/docket/CriminalCourtCases/caseInfo.asp?caseNumber="

# Cheap-but-effective browser-ish headers to avoid trivial blocks
STATIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Referer": "https://www.superiorcourt.maricopa.gov/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1",
}

def baserow_session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {TOKEN}"})
    return s

def list_rows(session, page_size=200):
    """Yield (row_id, fields_dict) for all rows in the table using pagination."""
    url = f"{BASEROW_API}/database/rows/table/{TABLE_ID}/?user_field_names=true&size={page_size}"
    while url:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for row in data.get("results", []):
            yield row["id"], row
        url = data.get("next")  # absolute URL to next page, if any

def update_row(session, row_id, fields):
    """PATCH specific fields on a row, using user field names."""
    if not fields:
        return
    url = f"{BASEROW_API}/database/rows/table/{TABLE_ID}/{row_id}/?user_field_names=true"
    resp = session.patch(url, json=fields, timeout=60)
    resp.raise_for_status()
    return resp.json()

def extract_charge_with_priority(soup):
    """Heuristic: prefer MURDER if present; else first charge in docket section."""
    charges_section = soup.find("div", id="tblDocket12")
    if not charges_section:
        return None

    rows = charges_section.find_all("div", class_="row g-0")
    first_charge = None
    for row in rows:
        divs = row.find_all("div")
        for d in divs:
            description = d.get_text(strip=True)
            if not description:
                continue
            # remember the first non-empty description we see
            if first_charge is None:
                first_charge = description
            if "MURDER" in description.upper():
                return description
    return first_charge

def extract_today_event(soup):
    """Look for a calendar/event row that matches today's date, return the event text if found."""
    # e.g., 9/5/2025 (avoid leading zeros in month/day)
    today_str = datetime.now().strftime("%-m/%-d/%Y").replace("/0", "/")
    calendar_section = soup.find("div", id="tblForms4")
    if not calendar_section:
        return None

    rows = calendar_section.find_all("div", class_="row g-0")
    for row in rows:
        cols = row.find_all("div")
        # This layout heuristic matches your earlier scraper approach
        if len(cols) >= 6 and cols[1].get_text(strip=True) == today_str:
            return cols[5].get_text(strip=True)
    return None

def page_has_error_message(soup):
    text = soup.get_text(separator="\n", strip=True).lower()
    for phrase in [
        "server is busy", "could not be found", "error has occurred",
        "unavailable", "temporarily unavailable", "try again later"
    ]:
        if phrase in text:
            return phrase
    return None

def main():
    api = baserow_session()
    http = requests.Session()
    http.headers.update(STATIC_HEADERS)

    updated = 0
    skipped = 0
    failed = 0

    for row_id, row in list_rows(api):
        case_number = str(row.get("Case #") or "").strip()
        if not case_number:
            continue

        url = BASE_URL + case_number
        try:
            r = http.get(url, timeout=60)
            print(f"[{case_number}] GET {r.status_code} {url}")
            if r.status_code != 200:
                print(f"[{case_number}] ❌ non-200; skipping")
                skipped += 1
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            bad = page_has_error_message(soup)
            if bad:
                print(f"[{case_number}] ❌ error phrase on page: {bad}; skipping")
                skipped += 1
                continue

            charge = extract_charge_with_priority(soup)
            today_event = extract_today_event(soup)

            patch = {}
            if charge:
                patch["Crime"] = charge
            if today_event:
                # if you prefer a different field for this, change "Time"
                patch["Time"] = today_event

            if patch:
                print(f"[{case_number}] ✅ PATCH row {row_id}: {patch}")
                update_row(api, row_id, patch)
                updated += 1
            else:
                print(f"[{case_number}] (no changes)")

        except requests.RequestException as e:
            print(f"[{case_number}] ❌ request failed: {e}")
            failed += 1

        time.sleep(4)  # be polite to the court site

    print(f"Done. Updated={updated}, Skipped={skipped}, Failed={failed}")

if __name__ == "__main__":
    main()
