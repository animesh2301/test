"""
Business Central -> CSV: vendor ledger entries for a list of PINV numbers.

For each PINV number below it calls

    GET .../api/microsoft/analytics/v1.0/companies({company})/vendorLedgerEntries
        ?$filter=purchInvHeaderDocumentNo eq '<PINV>'

follows paging, and writes every entry returned to ONE CSV file. Each row
also gets a "requested_pinv" column. PINVs with no entries are listed at
the end of the run.
"""

import csv
import os
import time

import requests
import urllib3

# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = "<tenant-id>"
CLIENT_ID = "<client-id>"
CLIENT_SECRET = "<client-secret>"
ENVIRONMENT = "<bc-environment>"
COMPANY_ID = "<bc-company-guid>"

API_HOST = "https://api.businesscentral.dynamics.com"
BASE_URL = (f"{API_HOST}/v2.0/{ENVIRONMENT}/api/microsoft/analytics/v1.0"
            f"/companies({COMPANY_ID})/vendorLedgerEntries")

PINV_NUMBERS = [
    "PINV-3631389",
    # "PINV-...",
]
# Or read them from a text file, one per line - leave "" to use the list above
PINV_FILE = ""

OUTPUT_CSV = os.path.join(os.path.expanduser("~"), "Downloads",
                          "vendorLedgerEntries.csv")

VERIFY_SSL = True          # False only for the local certificate issue
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# HELPERS
# ============================================================

http = requests.Session()
token = None


def get_token():
    resp = http.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET, "scope": f"{API_HOST}/.default"},
        timeout=60, verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get(url, params=None):
    """GET with retries for throttling (429), BC errors (5xx) and an expired token."""
    global token
    for attempt in range(1, 8):
        token = token or get_token()
        resp = http.get(url, params=params, timeout=120, verify=VERIFY_SSL,
                        headers={"Authorization": f"Bearer {token}",
                                 "Accept": "application/json"})
        if resp.status_code == 401:
            token = None
            continue
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = int(resp.headers.get("Retry-After", 10 * attempt))
            print(f"  BC {resp.status_code}, waiting {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"GET {resp.url} -> {resp.status_code}: {resp.text[:500]}")
        return resp.json()
    raise RuntimeError(f"GET {url}: gave up after retries")


def flatten(d, prefix=""):
    """Nested JSON objects -> flat columns (a.b); lists kept as text."""
    out = {}
    for k, v in d.items():
        if k.startswith("@odata"):
            continue
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        else:
            out[key] = v if not isinstance(v, list) else str(v)
    return out


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    pinvs = PINV_NUMBERS
    if PINV_FILE:
        with open(PINV_FILE) as f:
            pinvs = [line.strip() for line in f if line.strip()]

    rows, not_found = [], []
    for i, pinv in enumerate(pinvs, start=1):
        safe = pinv.replace("'", "''")
        page = get(BASE_URL, {"$filter": f"purchInvHeaderDocumentNo eq '{safe}'"})
        entries = []
        while True:
            entries += page.get("value", [])
            next_link = page.get("@odata.nextLink")
            if not next_link:
                break
            page = get(next_link)

        print(f"[{i}/{len(pinvs)}] {pinv}: {len(entries)} entr(y/ies)")
        if not entries:
            not_found.append(pinv)
        rows += [{"requested_pinv": pinv, **flatten(e)} for e in entries]

    # every column that appears in any row, in first-seen order
    columns = list(dict.fromkeys(c for r in rows for c in r)) or ["requested_pinv"]
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{len(rows)} row(s) for {len(pinvs)} PINV(s) -> {OUTPUT_CSV}")
    if not_found:
        print(f"No entries for: {', '.join(not_found)}")
