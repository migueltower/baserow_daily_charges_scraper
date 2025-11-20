# fetch_charges.py — Baserow version
# Reads rows from your Baserow "MC Daily" table (ID 709546),
# scrapes the court page for each "Case #",
# and writes back fields like "Crime" and "Time".
#
# Environment variables expected:
#   BASEROW_TOKEN   -> your Baserow database token (required)
#   BASEROW_TABLE_ID-> defaults to "709546" if not set
# fetch_charges_baserow.py — fully equivalent to Airtable version
# Uses case numbers from your Baserow "MC Daily" table,
# scrapes docket pages for charges and events,
# and writes them back to the same rows.

import os
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# ---------------------------
# CONFIGURATION
# ---------------------------
BASEROW_API = "https://api.baserow.io/api"
TABLE_ID = os.getenv("BASEROW_TABLE_ID", "709546")
TOKEN = os.environ["BASEROW_TOKEN"]  # will raise KeyError if missing

TOKEN = os.environ["BASEROW_TOKEN"]  # must be set in GitHub Actions secrets
BASE_URL = "https://www.superiorcourt.maricopa.gov/docket/CriminalCourtCases/caseInfo.asp?caseNumber="

# Cheap-but-effective browser-ish headers to avoid trivial blocks
STATIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/118.0.0.0 Safari/537.36",
"Referer": "https://www.superiorcourt.maricopa.gov/",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
"Accept-Language": "en-US,en;q=0.9",
"Connection": "keep-alive",
    "DNT": "1",
    "DNT": "1"
}
# Disable warnings when falling back to verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------
# BASEROW HELPERS
# ---------------------------
def baserow_session():
    """Return an authenticated session for Baserow API."""
s = requests.Session()
s.headers.update({"Authorization": f"Token {TOKEN}"})
return s


def list_rows(session, page_size=200):
    """Yield (row_id, fields_dict) for all rows in the table using pagination."""
    """Yield (row_id, fields_dict) for all rows in the table (paginated)."""
url = f"{BASEROW_API}/database/rows/table/{TABLE_ID}/?user_field_names=true&size={page_size}"
while url:
resp = session.get(url, timeout=60)
resp.raise_for_status()
data = resp.json()
for row in data.get("results", []):
            yield row["id"], row
        url = data.get("next")  # absolute URL to next page, if any
            yield row["id"], row.get("Case #"), row
        url = data.get("next")  # absolute URL if pagination continues


def update_row(session, row_id, fields):
    """PATCH specific fields on a row, using user field names."""
    """PATCH specified fields for a given row ID."""
if not fields:
return
url = f"{BASEROW_API}/database/rows/table/{TABLE_ID}/{row_id}/?user_field_names=true"
resp = session.patch(url, json=fields, timeout=60)
resp.raise_for_status()
return resp.json()


# ---------------------------
# SCRAPER HELPERS
# ---------------------------
def extract_charge_with_priority(soup):
    """Heuristic: prefer MURDER if present; else first charge in docket section."""
    """Prefer 'MURDER' if present; else first charge listed."""
charges_section = soup.find("div", id="tblDocket12")
if not charges_section:
return None
@@ -64,20 +74,21 @@ def extract_charge_with_priority(soup):
first_charge = None
for row in rows:
divs = row.find_all("div")
        for d in divs:
            description = d.get_text(strip=True)
            if not description:
        for i in range(len(divs)):
            text = divs[i].get_text(strip=True)
            if not text:
continue
            # remember the first non-empty description we see
            if first_charge is None:
                first_charge = description
            if "MURDER" in description.upper():
                return description
            if "Description" in text and i + 1 < len(divs):
                description = divs[i + 1].get_text(strip=True)
                if not first_charge:
                    first_charge = description
                if "MURDER" in description.upper():
                    return description
return first_charge


def extract_today_event(soup):
    """Look for a calendar/event row that matches today's date, return the event text if found."""
    # e.g., 9/5/2025 (avoid leading zeros in month/day)
    """Look for an event row that matches today's date."""
today_str = datetime.now().strftime("%-m/%-d/%Y").replace("/0", "/")
calendar_section = soup.find("div", id="tblForms4")
if not calendar_section:
@@ -86,22 +97,32 @@ def extract_today_event(soup):
rows = calendar_section.find_all("div", class_="row g-0")
for row in rows:
cols = row.find_all("div")
        # This layout heuristic matches your earlier scraper approach
if len(cols) >= 6 and cols[1].get_text(strip=True) == today_str:
return cols[5].get_text(strip=True)
return None

def page_has_error_message(soup):

def page_has_error_message(soup, case_number):
    """Check if the page shows any server or unavailable message."""
text = soup.get_text(separator="\n", strip=True).lower()
    for phrase in [
    known_fail_phrases = [
"server is busy", "could not be found", "error has occurred",
"unavailable", "temporarily unavailable", "try again later"
    ]:
    ]
    snippet = text[:300].replace("\n", " ")
    print(f"[{case_number}] Page snippet: {snippet}")
    for phrase in known_fail_phrases:
if phrase in text:
            return phrase
    return None
            print(f"[{case_number}] ❌ Skipping due to error message → '{phrase}'")
            return True
    return False


def main():
# ---------------------------
# CORE LOGIC
# ---------------------------
def process_cases():
    """Pull case numbers from Baserow, fetch their details, and update the rows."""
api = baserow_session()
http = requests.Session()
http.headers.update(STATIC_HEADERS)
@@ -110,52 +131,57 @@ def main():
skipped = 0
failed = 0

    for row_id, row in list_rows(api):
        case_number = str(row.get("Case #") or "").strip()
    for row_id, case_number, row in list_rows(api):
        case_number = str(case_number or "").strip()
if not case_number:
continue

        url = BASE_URL + case_number
        full_url = BASE_URL + case_number
try:
            r = http.get(url, timeout=60)
            print(f"[{case_number}] GET {r.status_code} {url}")
            if r.status_code != 200:
                print(f"[{case_number}] ❌ non-200; skipping")
            response = http.get(full_url, timeout=60)
            print(f"[{case_number}] 🔍 Status: {response.status_code}")
            if response.status_code != 200:
                print(f"[{case_number}] ❌ Failed with status {response.status_code}")
skipped += 1
continue

            soup = BeautifulSoup(r.text, "html.parser")
            soup = BeautifulSoup(response.text, "html.parser")

            bad = page_has_error_message(soup)
            if bad:
                print(f"[{case_number}] ❌ error phrase on page: {bad}; skipping")
            if page_has_error_message(soup, case_number):
                with open(f"error_page_{case_number}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
skipped += 1
continue

            # --- Extract details
charge = extract_charge_with_priority(soup)
today_event = extract_today_event(soup)

            patch = {}
            fields_to_update = {}
if charge:
                patch["Crime"] = charge
                fields_to_update["Crime"] = charge
if today_event:
                # if you prefer a different field for this, change "Time"
                patch["Time"] = today_event
                fields_to_update["Case Number Links"] = today_event  # same as Airtable

            if patch:
                print(f"[{case_number}] ✅ PATCH row {row_id}: {patch}")
                update_row(api, row_id, patch)
            if fields_to_update:
                print(f"[{case_number}] ✅ Updating Baserow row {row_id}: {fields_to_update}")
                update_row(api, row_id, fields_to_update)
updated += 1
else:
                print(f"[{case_number}] (no changes)")
                print(f"[{case_number}] No updates found.")
                skipped += 1

except requests.RequestException as e:
            print(f"[{case_number}] ❌ request failed: {e}")
            print(f"[{case_number}] ❌ Request error: {e}")
failed += 1

        time.sleep(4)  # be polite to the court site
        time.sleep(4)  # delay to avoid hammering the site

    print(f"✅ Done. Updated={updated}, Skipped={skipped}, Failed={failed}")

    print(f"Done. Updated={updated}, Skipped={skipped}, Failed={failed}")

# ---------------------------
# MAIN ENTRYPOINT
# ---------------------------
if __name__ == "__main__":
    main()
    process_cases() n   
