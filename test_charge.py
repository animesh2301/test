"""
Test script: post ONE PO invoice with Charge (Item) lines to Business Central.

Reads the invoice from the ContinuFlow tables for RECORD_ID below and builds
a DRAFT purchase invoice in BC:

    header
    for each GRN (receipt):
        charge line first, then the item lines      (custom getReceiptLines)
        Qty. to Match for item lines that differ    (custom purchaseLineMatches)
        read the lines -> item amounts -> shares
        charge spread over THIS receipt's items     (custom itemChargeAssignments)
    GL variance lines (Account + dimensions)

Company 889 only. On success sets ERP_POST = 'P'. Every step and any error
is printed to the output; nothing else is written to the CF tables.

No posting description, PDF or post - review the draft in BC.
Running it again creates a NEW draft (no resume): delete the old draft first.
"""

import sys
import time
import traceback

import pymssql
import requests
import urllib3

# ============================================================
# CONFIGURATION - hardcode for the test
# ============================================================

RECORD_ID = 123456                      # AP_DataConversions.RECORD_ID to post

TENANT_ID = "<tenant-id>"
CLIENT_ID = "<client-id>"
CLIENT_SECRET = "<client-secret>"
ENVIRONMENT = "<bc-environment>"

CF_CLIENT_ID = 363
CF_COMPANY_ID = 889
BC_COMPANY_ID = "<bc-company-guid-for-889>"

API_HOST = "https://api.businesscentral.dynamics.com"
STD_BASE = f"{API_HOST}/v2.0/{TENANT_ID}/{ENVIRONMENT}/api/v2.0"
CUSTOM_BASE = f"{API_HOST}/v2.0/{TENANT_ID}/{ENVIRONMENT}/api/custom/purchasing/v1.0"

SQL_SERVER = "<server>.database.windows.net"
SQL_DATABASE = "<database>"
SQL_USER = "<user>"
SQL_PASSWORD = "<password>"

COST_CENTER_CODE = "COST CENTER"
DIVISION_CODE = "DIVISION"

VERIFY_SSL = True                       # False only for the local cert issue
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# BC + SQL HELPERS
# ============================================================

http = requests.Session()
token = None


def get_token():
    resp = http.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET,
              "scope": f"{API_HOST}/.default"},
        timeout=60, verify=VERIFY_SSL)
    resp.raise_for_status()
    return resp.json()["access_token"]


def bc(method, url, body=None):
    """One BC call; waits and retries when BC is busy (429 / 5xx)."""
    global token
    for attempt in range(1, 6):
        token = token or get_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        if method == "PATCH":
            headers["If-Match"] = "*"
        resp = http.request(method, url, json=body, headers=headers,
                            timeout=120, verify=VERIFY_SSL)
        if resp.status_code == 401:
            token = None
            continue
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = int(resp.headers.get("Retry-After", 5 * attempt))
            print(f"  BC {resp.status_code}, waiting {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {url} -> {resp.status_code}: {resp.text[:1000]}")
        return resp.json() if resp.content else None
    raise RuntimeError(f"{method} {url}: gave up after 5 attempts")


def rows(cur):
    """Query result as dicts, column names upper-cased."""
    cols = [c[0].upper() for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def text(v):
    """Trimmed string, or None when empty."""
    v = str(v).strip() if v is not None else ""
    return v or None


def seq(v):
    """Receipt line no as text: 10000, 10000.0, "10000" -> "10000"."""
    return str(int(float(v)))


# ============================================================
# MAIN
# ============================================================

def main():
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USER,
                           password=SQL_PASSWORD, database=SQL_DATABASE)
    cur = conn.cursor()

    client_id, company_id = CF_CLIENT_ID, CF_COMPANY_ID
    api = f"{STD_BASE}/companies({BC_COMPANY_ID})"
    custom = f"{CUSTOM_BASE}/companies({BC_COMPANY_ID})"

    # ---- read the invoice from ContinuFlow -------------------------------
    cur.execute("SELECT TOP 1 * FROM dbo.AP_DC_InvoiceHeader "
                "WHERE RECORD_ID = %s ORDER BY ID DESC", (RECORD_ID,))
    header = rows(cur)[0]

    cur.execute(
        """
        SELECT g.*, d.DivisionValue AS MST_DIVISION_VALUE
        FROM dbo.AP_DC_GL_Details g
        OUTER APPLY (SELECT TOP 1 m.DivisionValue FROM dbo.mst_divisions m
                     WHERE m.CLIENT_ID = %s AND m.COMPANY_ID = %s
                       AND m.UIDivision = g.DIVISION
                     ORDER BY m.DivisionValue) d
        WHERE g.RECORD_ID = %s
        ORDER BY g.SERIAL_NO, g.ID
        """, (client_id, company_id, RECORD_ID))
    lines = rows(cur)

    receipt_lines = [l for l in lines if not text(l.get("GLCODE"))]
    variance_lines = [l for l in lines if text(l.get("GLCODE"))]
    grns = list(dict.fromkeys(text(l.get("GRN_NUMBER")) for l in receipt_lines))

    # GRN -> receipt (PREC); (GRN, line no) -> line type (Item / Charge)
    prec_of, type_of = {}, {}
    if grns:
        cur.execute(
            f"""
            SELECT r.orderNumber, r.[number], rl.[sequence], rl.lineType
            FROM dbo.AP_DC_PurchaseReceipts r
            LEFT JOIN dbo.AP_DC_PurchaseReceiptLines rl
              ON rl.documentId = r.id
             AND rl.CLIENT_ID = r.CLIENT_ID AND rl.COMPANY_ID = r.COMPANY_ID
            WHERE r.CLIENT_ID = %s AND r.COMPANY_ID = %s
              AND r.orderNumber IN ({", ".join("%s" for _ in grns)})
            """, (client_id, company_id, *grns))
        for grn, prec, line_no, line_type in cur.fetchall():
            prec_of[str(grn).strip()] = str(prec)
            if line_no is not None:
                type_of[(str(grn).strip(), seq(line_no))] = str(line_type or "")

    def is_charge(l):
        return "CHARGE" in type_of.get((text(l.get("GRN_NUMBER")),
                                        seq(l.get("SEQUENCE"))), "").upper()

    print(f"RECORD_ID {RECORD_ID}: {len(receipt_lines)} receipt line(s) on "
          f"GRN {grns} -> {prec_of}, {len(variance_lines)} variance line(s)")

    def step(name, outcome, detail=""):
        print(f"  {name}: {outcome} {detail}")

    try:
        # ---- header ------------------------------------------------------
        body = {"vendorNumber": str(header.get("VEND_CODE") or "").strip()}
        if header.get("INVOICE_NUMBER"):
            body["vendorInvoiceNumber"] = str(header["INVOICE_NUMBER"])
        if header.get("INVOICE_DATE"):
            body["invoiceDate"] = str(header["INVOICE_DATE"])[:10]
        if header.get("POSTING_DATE"):
            body["postingDate"] = str(header["POSTING_DATE"])[:10]
        if header.get("CURRENCY"):
            body["currencyCode"] = "USD"
        created = bc("POST", f"{api}/purchaseInvoices", body)
        invoice_id, pinv = created["id"], created.get("number")
        step("create_header", "ok", f"{pinv} {invoice_id}")

        lines_url = f"{api}/purchaseInvoices({invoice_id})/purchaseInvoiceLines"
        known = {x["id"] for x in bc("GET", lines_url)["value"]}

        # ---- receipt lines, GRN by GRN -----------------------------------
        for grn in grns:
            receipt_no = prec_of[grn]
            group = [l for l in receipt_lines if text(l.get("GRN_NUMBER")) == grn]
            group.sort(key=lambda l: not is_charge(l))      # charge line first

            # inner loop 1: add the lines, remember the charge and item lines
            charge_line_id, item_line_ids = None, []
            for l in group:
                line_no = seq(l.get("SEQUENCE"))
                bc("POST", f"{custom}/receiptInvoices({invoice_id})"
                           f"/Microsoft.NAV.getReceiptLines",
                   {"receiptLineNos": line_no, "receiptNo": receipt_no})
                after = bc("GET", lines_url)["value"]
                new = [x for x in after if x["id"] not in known
                       and x.get("lineType") != "Comment"]
                known = {x["id"] for x in after}
                if not new:
                    raise RuntimeError(f"getReceiptLines {receipt_no} line {line_no} "
                                       f"added no line")
                line = new[0]
                step(f"line_{l.get('ID')}", "ok",
                     f"{line['id']} {receipt_no} line {line_no} {line.get('lineType')}")

                if is_charge(l):
                    charge_line_id = line["id"]
                    continue
                item_line_ids.append(line["id"])

                # Qty. to Match only when the GL quantity differs
                if l.get("QUANTITY") is not None and \
                        round(float(line.get("quantity") or 0), 5) != round(float(l["QUANTITY"]), 5):
                    bc("PATCH", f"{custom}/purchaseLineMatches({line['id']})",
                       {"dcgcQtyToMatch": float(l["QUANTITY"])})
                    step(f"qty_{l.get('ID')}", "ok",
                         f"{line.get('quantity')} -> {float(l['QUANTITY'])}")

            if not charge_line_id:
                continue                                    # no charge on this receipt

            # amounts AFTER the qty patches; line nos to match the assignment rows
            now = {x["id"]: x for x in bc("GET", lines_url)["value"]}
            charge_no = int(now[charge_line_id]["sequence"])
            amount = {int(now[i]["sequence"]): float(now[i].get("amountExcludingTax") or 0)
                      for i in item_line_ids}
            total = sum(amount.values())
            if total <= 0:
                raise RuntimeError(f"GRN {grn}: item total is 0")

            # share per item line no; last one takes the rounding remainder
            share = {}
            nos = list(amount)
            for no in nos[:-1]:
                share[no] = round(amount[no] / total, 5)
            share[nos[-1]] = round(1 - sum(share.values()), 5)

            # inner loop 2: this charge's rows for this receipt's items only
            assignments = bc("GET", f"{custom}/itemChargeAssignments"
                                    f"?$filter=documentNo eq '{pinv}'")["value"]
            for row in assignments:
                if int(row["documentLineNo"]) != charge_no:
                    continue                                # another receipt's charge
                applies_to = int(row["appliesToDocLineNo"])
                if applies_to not in share:
                    continue                                # another receipt's item
                bc("PATCH", f"{custom}/itemChargeAssignments({row['id']})",
                   {"quantityToAssign": share[applies_to]})
            step(f"assign_{grn}", "ok", f"charge line {charge_no}, total {total}: {share}")

        # ---- GL variance lines -------------------------------------------
        for l in variance_lines:
            code = text(l.get("GLCODE"))
            cur.execute("SELECT TOP 1 ACCOUNT_NUMBER FROM dbo.COA_Master_SAP "
                        "WHERE CLIENT_ID = %s AND COMPANY_ID = %s AND ACCOUNT_CODE = %s "
                        "AND ACCOUNT_NUMBER IS NOT NULL", (client_id, company_id, code))
            row = cur.fetchone()
            account_id = str(row[0]).strip() if row else \
                bc("GET", f"{api}/accounts?$filter=number eq '{code}'")["value"][0]["id"]

            new_line = bc("POST", lines_url, {
                "lineType": "Account",
                "accountId": account_id,
                "description": str(text(l.get("DESCRIPTION")) or l.get("GL_DESCR") or "")[:100],
                "quantity": 1,
                "unitCost": float(l.get("AMOUNT") or 0),
            })
            step(f"line_{l.get('ID')}", "ok", f"{new_line['id']} GL {code} {l.get('AMOUNT')}")
            for dim_code, value in ((COST_CENTER_CODE, text(l.get("COST_CENTER"))),
                                    (DIVISION_CODE, text(l.get("MST_DIVISION_VALUE"))
                                     or text(l.get("DIVISION")))):
                if value:
                    bc("POST", f"{lines_url}({new_line['id']})/dimensionSetLines",
                       {"code": dim_code, "valueCode": value})

        # ---- write back: ERP_POST = 'P' ----------------------------------
        cur.execute("UPDATE dbo.AP_DataConversions SET ERP_POST = 'P', ERP_ERROR = NULL "
                    "WHERE RECORD_ID = %s", (RECORD_ID,))
        conn.commit()
        step("erp_post", "ok", "ERP_POST set to P")
        print(f"\nDone: draft {pinv} - review it in BC.")

    except Exception as e:
        traceback.print_exc()
        print(f"\nFailed: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
