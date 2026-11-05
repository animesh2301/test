"""
ContinuFlow tables -> Business Central: invoice posting.

AP_DataConversions.INVOICE_TYPE picks the path (run_batch):
  'PO'                      process_po_record - receipt lines (client's
                                                getReceiptLines + Qty. to
                                                Match) + GL variance lines
  anything else ('NON PO')  process_record    - Account lines

    AP_DataConversions   CLIENT_ID / COMPANY_ID in scope,
                         STATUS = 'APPROVED', ERP_POST = 'N'
        -> AP_DC_InvoiceHeader   (one header per RECORD_ID)
        -> AP_DC_GL_Details      (every row is an invoice line;
                                  COST_CENTER and DIVISION are dimensions)
           + mst_divisions       (DIVISION = UIDivision -> DivisionValue)
            -> BC purchaseInvoices + purchaseInvoiceLines
               + dimensionSetLines for COST CENTER / DIVISION
               + invoice PDF + posting description
        -> ERP_POST = 'P' (posted) / 'E' (retries used up)

Retries are safe. Every BC identifier is written to bc.InvoicePostLog the
moment it exists, so a retry resumes the half-built invoice instead of
creating a second one.
"""

import logging
import re

import requests

import uat_config as cfg
from bc_http import BcError, bc_get, bc_request

log = logging.getLogger("invoice_poster")

GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")


class Held(Exception):
    """Needs a person (missing or wrong data)."""


def _rows(cursor):
    """Rows as dicts, column names upper-cased (Description -> DESCRIPTION)."""
    cols = [c[0].upper() for c in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]


def _text(v):
    """Trimmed string, or None when empty."""
    v = str(v).strip() if v is not None else ""
    return v or None


# ---------------------------------------------------------------------------
# Shared by both invoice types
# ---------------------------------------------------------------------------

def pending_records(conn, limit=None) -> list[dict]:
    """Approved, not yet posted, in scope - PO and non-PO."""
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT TOP (%s) dc.RECORD_ID, dc.CLIENT_ID, dc.COMPANY_ID,
                        dc.DOC_NO, dc.INVOICE_TYPE, dc.STATUS, dc.ERP_POST
        FROM dbo.AP_DataConversions dc
        WHERE {cfg.company_scope_sql('dc')}
          AND dc.STATUS   = %s
          AND dc.ERP_POST = %s
        ORDER BY dc.RECORD_ID
        """,
        (limit or cfg.BATCH_SIZE, cfg.DC_STATUS, cfg.DC_ERP_POST_PENDING),
    )
    return _rows(cur)


def start_attempt(conn, dc):
    """
    (attempt number, step) for one record. step(name, outcome, ...) writes
    one bc.InvoicePostLog row (LoggedAt defaults to UTC).
    """
    cur = conn.cursor()
    cur.execute("SELECT ISNULL(MAX(Attempt), 0) + 1 FROM bc.InvoicePostLog "
                "WHERE RecordId = %s", (dc["RECORD_ID"],))
    attempt = int(cur.fetchone()[0])

    def step(name, outcome, detail=None, inv_id=None, inv_no=None):
        conn.cursor().execute(
            """
            INSERT INTO bc.InvoicePostLog
                (RecordId, ClientId, CompanyId, Attempt, Step, Outcome, Detail,
                 BcInvoiceId, BcInvoiceNumber)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (dc["RECORD_ID"], dc["CLIENT_ID"], dc["COMPANY_ID"], attempt, name,
             outcome, str(detail or "")[:4000], inv_id, inv_no))
        conn.commit()

    return attempt, step


def load_invoice(conn, dc):
    """
    (header, lines). Lines carry DIVISION matched to mst_divisions.UIDivision
    for this client/company: MST_DIVISION_VALUE and MST_LOCATION_ID. OUTER
    APPLY + TOP 1, so a line with no match keeps its row and a duplicate in
    mst_divisions never duplicates a line.
    """
    cur = conn.cursor()
    cur.execute("SELECT TOP 1 * FROM dbo.AP_DC_InvoiceHeader "
                "WHERE RECORD_ID = %s ORDER BY ID DESC", (dc["RECORD_ID"],))
    header = (_rows(cur) or [None])[0]

    cur.execute(
        """
        SELECT g.*, d.DivisionValue AS MST_DIVISION_VALUE,
               d.locationId AS MST_LOCATION_ID
        FROM dbo.AP_DC_GL_Details g
        OUTER APPLY (
            SELECT TOP 1 m.DivisionValue, m.locationId
            FROM dbo.mst_divisions m
            WHERE m.CLIENT_ID = %s AND m.COMPANY_ID = %s
              AND m.UIDivision = g.DIVISION
            ORDER BY m.DivisionValue
        ) d
        WHERE g.RECORD_ID = %s
        ORDER BY g.SERIAL_NO, g.ID
        """,
        (dc["CLIENT_ID"], dc["COMPANY_ID"], dc["RECORD_ID"]))
    return header, _rows(cur)


def resume_invoice_id(conn, record_id):
    """The BC invoice an earlier attempt created, so a retry never makes a second."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT TOP 1 BcInvoiceId FROM bc.InvoicePostLog
        WHERE RecordId = %s AND Step = 'create_header' AND Outcome = 'ok'
          AND BcInvoiceId IS NOT NULL
        ORDER BY LogId DESC
        """, (record_id,))
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def resolve_account(conn, client_id, company_id, gl_code, bc_guid):
    """
    GLCODE (e.g. 61000) -> BC account id, from COA_Master_SAP for THIS
    client and company (the same number has a different id per company).
    Falls back to BC when COA_Master_SAP has no usable row. Two different ids
    for one code is ambiguous, so the invoice is held rather than guessing.
    """
    code = str(gl_code or "").strip()
    if not code:
        return None, "no GLCODE on the line"

    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ACCOUNT_NUMBER
        FROM dbo.COA_Master_SAP
        WHERE CLIENT_ID = %s AND COMPANY_ID = %s AND ACCOUNT_CODE = %s
          AND ACCOUNT_NUMBER IS NOT NULL
        """,
        (client_id, company_id, code),
    )
    ids = {str(r[0]).strip() for r in cur.fetchall()
           if r[0] and GUID_RE.match(str(r[0]).strip())}

    if len(ids) == 1:
        return ids.pop(), None
    if len(ids) > 1:
        return None, (f"G/L {code} has {len(ids)} different BC ids in "
                      f"COA_Master_SAP for company {company_id}: "
                      f"{sorted(ids)}")

    url = (f"{cfg.BC_API_BASE}/companies({bc_guid})/accounts"
           f"?$filter=number eq '{code.replace(chr(39), chr(39) * 2)}'")
    vals = (bc_get(url, timeout=cfg.BC_HTTP_TIMEOUT) or {}).get("value", [])
    if vals:
        return vals[0]["id"], None

    return None, (f"G/L account {code} not found for company {company_id} "
                  f"in COA_Master_SAP or in BC")


def post_dimensions(api, invoice_id, line_id, l, step) -> None:
    """COST CENTER and DIVISION (mst_divisions value, else the line's own)."""
    division = _text(l.get("MST_DIVISION_VALUE")) or _text(l.get("DIVISION"))
    for enabled, code, value in (
        (cfg.POST_COST_CENTER, cfg.COST_CENTER_DIMENSION_CODE,
         _text(l.get("COST_CENTER"))),
        (cfg.POST_DIVISION, cfg.DIVISION_DIMENSION_CODE, division),
    ):
        if not enabled:
            continue
        if not value:
            step(f"dim_{code}", "skipped", "no value on the line")
            continue
        try:
            bc_request("POST", f"{api}/purchaseInvoices({invoice_id})"
                       f"/purchaseInvoiceLines({line_id})/dimensionSetLines",
                       json_body={"code": code, "valueCode": value},
                       timeout=cfg.BC_HTTP_TIMEOUT)
            step(f"dim_{code}", "ok", f"{code} = {value}")
        except BcError as e:
            step(f"dim_{code}", "error", f"{code} = {value}: {e.body or e}")
            if cfg.DIMENSION_FAILURE_IS_FATAL:
                raise Held(f"dimension {code} = {value} rejected by BC: "
                           f"{e.body or e}")


def attach_invoice_pdf(bc_guid, invoice_id, record_id, step) -> None:
    """Invoice PDF from the ContinuFlow API onto the BC invoice (once)."""
    invoice_url = (f"{cfg.BC_API_BASE}/companies({bc_guid})"
                   f"/purchaseInvoices({invoice_id})")
    base = f"{invoice_url}/documentAttachments"
    if (bc_get(base, timeout=cfg.BC_HTTP_TIMEOUT) or {}).get("value"):
        step("attachment", "skipped", "already attached")
        return

    invoice = bc_get(invoice_url, timeout=cfg.BC_HTTP_TIMEOUT) or {}
    filename = f"{invoice.get('number')}.pdf"

    # ContinuFlow API: login -> signed URL -> file
    login = requests.post(cfg.CF_API_LOGIN_URL, timeout=60, json={
        "loginBody": {"Source": "password",
                      "UserName": cfg.CF_API_USERNAME,
                      "Password": cfg.CF_API_PASSWORD}})
    login.raise_for_status()
    token = login.json()["token"]["token"]
    r = requests.get(cfg.CF_API_FILE_URL.format(record_id=record_id),
                     headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    file = requests.get(r.json()["signedUrl"], timeout=60)
    file.raise_for_status()
    content = file.content

    att = bc_request("POST", base, timeout=cfg.BC_HTTP_TIMEOUT, json_body={
        "fileName": filename,
        "parentType": "Purchase_x0020_Invoice",
        "parentId": invoice_id})
    bc_request("PATCH", f"{base}({att['id']})/attachmentContent",
               data=content, etag="*", timeout=cfg.BC_HTTP_TIMEOUT)
    step("attachment", "ok", f"{filename}, {len(content)} bytes")


def write_audit_trail(conn, record_id, comments, changes) -> None:
    """
    One row in the CF team's dbo.AP_AuditTrails (CREATED_ON in UTC).
    Never raises: ERP_POST is already committed when this runs, and a failed
    audit insert must not turn a posted invoice into a retry.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.AP_AuditTrails
                ([DESCRIPTION], [APPROVED_BY], [COMMENTS], [CREATED_BY],
                 [CREATED_ON], [LASTMODIFIED_BY], [LASTMODIFIED_ON],
                 [RECORD_ID], [ASSIGNED_TO], [CHANGES])
            VALUES (%s, %s, %s, NULL,
                    CAST(SYSUTCDATETIME() AS smalldatetime),
                    NULL, NULL, %s, NULL, %s)
            """,
            (cfg.AUDIT_DESCRIPTION[:cfg.AUDIT_DESCRIPTION_MAX],
             cfg.AUDIT_USER_ID,
             str(comments)[:cfg.AUDIT_COMMENTS_MAX] if comments else None,
             record_id,
             str(changes)[:cfg.AUDIT_CHANGES_MAX] if changes else None),
        )
        conn.commit()
    except Exception:
        log.exception("Could not write AP_AuditTrails row for record %s",
                      record_id)
        try:
            conn.rollback()
        except Exception:
            pass


def mark_posted(conn, record_id, invoice_ref) -> None:
    """ERP_POST -> done, then the CF audit row."""
    cur = conn.cursor()
    cur.execute("UPDATE dbo.AP_DataConversions "
                "SET ERP_POST = %s, ERP_ERROR = NULL WHERE RECORD_ID = %s",
                (cfg.DC_ERP_POST_DONE, record_id))
    conn.commit()
    write_audit_trail(conn, record_id,
                      cfg.AUDIT_POSTED_COMMENTS.format(invoice_number=invoice_ref),
                      cfg.AUDIT_POSTED_CHANGES)


def mark_failed(conn, record_id, attempt, e, step) -> dict:
    """
    Log the failure, latest error to ERP_ERROR (shows in the ContinuFlow UI)
    and the CF audit row. After MAX_ATTEMPTS, ERP_POST goes to the failed
    marker so the timer stops retrying.
    """
    if isinstance(e, Held):
        status, message = "held", f"HELD: {e}"
        step("held", "held", str(e))
        log.warning("Record %s held: %s", record_id, e)
    elif isinstance(e, BcError):
        status, message = "bc_error", f"{e} :: {e.body}"
        step("bc_error", "error", message)
        log.exception("Record %s BC error", record_id)
    else:
        status, message = "error", str(e)
        step("error", "error", message)
        log.exception("Record %s failed", record_id)

    exhausted = attempt >= cfg.MAX_ATTEMPTS
    new_state = cfg.DC_ERP_POST_FAILED if exhausted else cfg.DC_ERP_POST_PENDING
    cur = conn.cursor()
    cur.execute("UPDATE dbo.AP_DataConversions "
                "SET ERP_ERROR = %s, ERP_POST = %s WHERE RECORD_ID = %s",
                (f"[attempt {attempt}] {message}"[:199], new_state, record_id))
    conn.commit()
    write_audit_trail(
        conn, record_id, cfg.AUDIT_FAILED_COMMENTS.format(message=message),
        cfg.AUDIT_FAILED_CHANGES
        if new_state != cfg.DC_ERP_POST_PENDING else cfg.AUDIT_RETRY_CHANGES)

    return {"recordId": record_id, "status": status, "error": message,
            "attempt": attempt, "retriesExhausted": exhausted}


# ---------------------------------------------------------------------------
# NON PO invoice - Account lines
# ---------------------------------------------------------------------------

def process_record(conn, dc: dict) -> dict:
    record_id = dc["RECORD_ID"]
    attempt, step = start_attempt(conn, dc)

    try:
        bc_guid = cfg.bc_company_guid(dc["COMPANY_ID"])
        api = f"{cfg.BC_API_BASE}/companies({bc_guid})"

        header, lines = load_invoice(conn, dc)
        if header is None:
            raise Held(f"no AP_DC_InvoiceHeader row for RECORD_ID {record_id}")
        step("load", "ok",
             f"header ID {header.get('ID')}, {len(lines)} line(s) to post")
        if not lines:
            raise Held("no AP_DC_GL_Details rows for this record")

        # ---- resolve every account before creating anything -------------
        accounts = []
        for l in lines:
            account_id, err = resolve_account(conn, dc["CLIENT_ID"],
                                              dc["COMPANY_ID"],
                                              l.get("GLCODE"), bc_guid)
            if err:
                raise Held(f"line SERIAL_NO {l.get('SERIAL_NO')}: {err}")
            accounts.append(account_id)
        step("resolve_accounts", "ok", f"{len(accounts)} account(s)")

        # ---- header (resume if a previous attempt made one) -------------
        invoice_id = resume_invoice_id(conn, record_id)
        if invoice_id:
            step("create_header", "skipped",
                 f"resuming invoice {invoice_id} from an earlier attempt",
                 inv_id=invoice_id)
        else:
            body = {"vendorNumber": str(header.get("VEND_CODE") or "").strip()}
            if header.get("INVOICE_NUMBER"):
                body["vendorInvoiceNumber"] = str(header["INVOICE_NUMBER"])
            if header.get("INVOICE_DATE"):
                body["invoiceDate"] = str(header["INVOICE_DATE"])[:10]
            if header.get("POSTING_DATE"):
                body["postingDate"] = str(header["POSTING_DATE"])[:10]
            if header.get("CURRENCY"):
                body["currencyCode"] = "USD"
            if not body["vendorNumber"]:
                raise Held("VEND_CODE is empty on AP_DC_InvoiceHeader")

            created = bc_request("POST", f"{api}/purchaseInvoices",
                                 json_body=body, timeout=cfg.BC_HTTP_TIMEOUT)
            invoice_id = created["id"]
            step("create_header", "ok", str(body),
                 inv_id=invoice_id, inv_no=created.get("number"))

        # ---- lines + dimensions (skip lines an earlier attempt made) ----
        lines_url = f"{api}/purchaseInvoices({invoice_id})/purchaseInvoiceLines"
        existing = (bc_get(lines_url, timeout=cfg.BC_HTTP_TIMEOUT)
                    or {}).get("value", [])

        for idx, l in enumerate(lines):
            if idx < len(existing):
                continue
            new_line = bc_request("POST", lines_url, json_body={
                "lineType": "Account",
                "accountId": accounts[idx],
                "description": str(l.get("GL_DESCR") or "")[:100],
                "quantity": 1,
                "unitCost": float(l.get("AMOUNT") or 0),
            }, timeout=cfg.BC_HTTP_TIMEOUT)
            step(f"line_{l.get('SERIAL_NO') or idx + 1}", "ok",
                 f"GL {l.get('GLCODE')} requested {l.get('AMOUNT')} "
                 f"stored {(new_line or {}).get('unitCost')}")
            post_dimensions(api, invoice_id, new_line["id"], l, step)

        # ---- invoice PDF ------------------------------------------------
        attach_invoice_pdf(bc_guid, invoice_id, record_id, step)

        # ---- posting description (client's custom API) ------------------
        post_desc = str(header["DESCRIPTION"])
        if cfg.CUSTOM_API_BASE:
            bc_request("PATCH", f"{cfg.CUSTOM_API_BASE}/companies({bc_guid})"
                       f"/{cfg.CUSTOM_INVOICE_ENTITY}({invoice_id})",
                       json_body={cfg.POSTING_DESC_FIELD: post_desc},
                       etag="*", timeout=cfg.BC_HTTP_TIMEOUT)
            step("posting_description", "ok",
                 f"{cfg.POSTING_DESC_FIELD} = {post_desc}")
        else:
            step("posting_description", "skipped", "BC_CUSTOM_API_PATH not set")

        invoice = bc_get(f"{api}/purchaseInvoices({invoice_id})",
                         timeout=cfg.BC_HTTP_TIMEOUT) or {}
        number = invoice.get("number")

        # ---- post, or leave as a draft ----------------------------------
        if cfg.POST_MODE == "post":
            bc_request("POST", f"{api}/purchaseInvoices({invoice_id})"
                       f"/Microsoft.NAV.post", timeout=cfg.BC_HTTP_TIMEOUT)
            step("post_invoice", "ok", number, inv_id=invoice_id, inv_no=number)
            outcome = "posted"
        else:
            step("post_invoice", "skipped",
                 "AP_POST_MODE=draft - a BC user reviews and posts",
                 inv_id=invoice_id, inv_no=number)
            outcome = "draft_created"

        mark_posted(conn, record_id, number or invoice_id)
        step("erp_post", "ok", f"ERP_POST set to {cfg.DC_ERP_POST_DONE}")

        return {"recordId": record_id, "invoiceType": "NON PO",
                "status": outcome, "invoiceId": invoice_id,
                "invoiceNumber": number, "lines": len(lines),
                "totalAmountExcludingTax":
                    invoice.get("totalAmountExcludingTax")}

    except Exception as e:
        return mark_failed(conn, record_id, attempt, e, step)


# ---------------------------------------------------------------------------
# PO invoice - one loop over GL_Details, in CF order:
#   item line (no GLCODE) -> client's getReceiptLines (receipt from the line's
#                            GRN_NUMBER, line no = SEQUENCE), then Qty. to
#                            Match = QUANTITY via purchaseLineMatches
#   GL variance (GLCODE)  -> Account line + dimensions, as non-PO
# ---------------------------------------------------------------------------

def process_po_record(conn, dc: dict) -> dict:
    # dc        = one AP_DataConversions row (RECORD_ID, CLIENT_ID, COMPANY_ID, ...)
    # attempt   = this try's number (1, 2, 3...), from bc.InvoicePostLog
    # step(...) = writes one bc.InvoicePostLog row for this record and attempt
    record_id = dc["RECORD_ID"]
    attempt, step = start_attempt(conn, dc)

    try:
        # bc_guid = BC company id for COMPANY_ID 889 / 890
        # api     = standard BC API base for this company
        # custom  = client's custom API base for this company
        bc_guid = cfg.bc_company_guid(dc["COMPANY_ID"])
        api = f"{cfg.BC_API_BASE}/companies({bc_guid})"
        custom = f"{cfg.CUSTOM_API_BASE}/companies({bc_guid})"

        # header = the AP_DC_InvoiceHeader row (dict: column -> value)
        # lines  = the AP_DC_GL_Details rows, in SERIAL_NO order (list of dicts);
        #          l.get("COLUMN") reads one column of a line, None if empty
        header, lines = load_invoice(conn, dc)
        if header is None:
            raise Held(f"no AP_DC_InvoiceHeader row for RECORD_ID {record_id}")
        step("load", "ok",
             f"PO invoice, header ID {header.get('ID')}, {len(lines)} line(s)")
        if not lines:
            raise Held("no AP_DC_GL_Details rows for this record")

        # ---- lookups, before anything is created in BC ------------------
        # grns    = the distinct GRN_NUMBERs on the item lines, e.g. ["PO-1001", "PO-1002"]
        # prec_of = GRN -> its receipt number, e.g. {"PO-1001": "PREC-0176723"}
        # (_text(v) = v trimmed, or None if empty)
        grns = sorted({_text(l.get("GRN_NUMBER")) for l in lines
                       if not _text(l.get("GLCODE"))})
        prec_of = {}
        cur = conn.cursor()
        if grns:
            cur.execute(
                f"""
                SELECT orderNumber, [number] FROM dbo.AP_DC_PurchaseReceipts
                WHERE CLIENT_ID = %s AND COMPANY_ID = %s
                  AND orderNumber IN ({", ".join("%s" for _ in grns)})
                """,
                (dc["CLIENT_ID"], dc["COMPANY_ID"], *grns))
            prec_of = {str(r[0]).strip(): str(r[1]) for r in cur.fetchall()}
            missing = [g for g in grns if g not in prec_of]
            if missing:
                raise Held(f"no purchase receipt for GRN {', '.join(missing)}")

        # accounts = GL_Details ID -> BC account id, for each GL variance line
        accounts = {}
        for l in lines:
            if _text(l.get("GLCODE")):
                account_id, err = resolve_account(conn, dc["CLIENT_ID"],
                                                  dc["COMPANY_ID"],
                                                  l.get("GLCODE"), bc_guid)
                if err:
                    raise Held(f"line SERIAL_NO {l.get('SERIAL_NO')}: {err}")
                accounts[l.get("ID")] = account_id

        # ---- header (resume if a previous attempt made one) -------------
        # invoice_id = BC id of the purchase invoice: the one an earlier attempt
        #              created (resume_invoice_id), or a new one created here
        invoice_id = resume_invoice_id(conn, record_id)
        if invoice_id:
            step("create_header", "skipped",
                 f"resuming invoice {invoice_id} from an earlier attempt",
                 inv_id=invoice_id)
        else:
            # body = the header fields sent to BC
            body = {"vendorNumber": str(header.get("VEND_CODE") or "").strip()}
            if header.get("INVOICE_NUMBER"):
                body["vendorInvoiceNumber"] = str(header["INVOICE_NUMBER"])
            if header.get("INVOICE_DATE"):
                body["invoiceDate"] = str(header["INVOICE_DATE"])[:10]
            if header.get("POSTING_DATE"):
                body["postingDate"] = str(header["POSTING_DATE"])[:10]
            if header.get("CURRENCY"):
                body["currencyCode"] = "USD"
            if not body["vendorNumber"]:
                raise Held("VEND_CODE is empty on AP_DC_InvoiceHeader")

            # created = BC's reply: the new invoice (id, number, ...)
            created = bc_request("POST", f"{api}/purchaseInvoices",
                                 json_body=body, timeout=cfg.BC_HTTP_TIMEOUT)
            invoice_id = created["id"]
            step("create_header", "ok", str(body),
                 inv_id=invoice_id, inv_no=created.get("number"))

        # done = lines already created by an earlier attempt:
        #        "line_<GL_Details ID>" -> BC line id, e.g. {"line_11": "7b3e..."}
        # Lines an earlier attempt already created on this invoice
        # (step line_<GL_Details ID>, Detail starts with the BC line id),
        # so a retry never adds a line twice.
        cur.execute("SELECT Step, Detail FROM bc.InvoicePostLog "
                    "WHERE RecordId = %s AND BcInvoiceId = %s AND Outcome = 'ok'",
                    (record_id, invoice_id))
        done = {r[0]: str(r[1]).split(" ")[0] for r in cur.fetchall()
                if str(r[0]).startswith("line_")}

        # lines_url = this invoice's lines in BC
        # bc_qty    = BC line id -> its quantity, for the lines on the invoice;
        #             a line id not in here after getReceiptLines is the line
        #             it just added
        lines_url = f"{api}/purchaseInvoices({invoice_id})/purchaseInvoiceLines"
        bc_qty = {x["id"]: x.get("quantity") for x in (bc_get(
            lines_url, timeout=cfg.BC_HTTP_TIMEOUT) or {}).get("value", [])}

        # ---- lines, in GL_Details order ---------------------------------
        for l in lines:
            # key     = this line's name in the log, e.g. "line_11"
            # line_id = its BC line id if an earlier attempt made it, else None
            key = f"line_{l.get('ID')}"
            line_id = done.get(key)

            if not _text(l.get("GLCODE")):
                # item line
                if not line_id:
                    # receipt_no = this line's receipt (PREC), from its GRN
                    # line_no    = the receipt line no, from SEQUENCE ("10000")
                    receipt_no = prec_of[_text(l.get("GRN_NUMBER"))]
                    line_no = str(int(float(l.get("SEQUENCE"))))
                    bc_request("POST", f"{custom}/receiptInvoices({invoice_id})"
                               f"/Microsoft.NAV.getReceiptLines",
                               json_body={"receiptLinesNos": line_no,
                                          "receiptNo": receipt_no},
                               timeout=cfg.BC_HTTP_TIMEOUT)
                    # the new line = the one not there before (BC may also
                    # add a "Receipt No. ...:" comment line - ignored)
                    # after = the invoice's lines now
                    # new   = ids in after but not in bc_qty (not comment lines)
                    after = (bc_get(lines_url, timeout=cfg.BC_HTTP_TIMEOUT)
                             or {}).get("value", [])
                    new = [x["id"] for x in after if x["id"] not in bc_qty
                           and x.get("lineType") != "Comment"]
                    bc_qty = {x["id"]: x.get("quantity") for x in after}
                    if not new:
                        raise Held(f"getReceiptLines {receipt_no} line {line_no} "
                                   f"added no line for SERIAL_NO "
                                   f"{l.get('SERIAL_NO')}")
                    line_id = new[0]
                    step(key, "ok", f"{line_id} {receipt_no} line {line_no}",
                         inv_id=invoice_id)

                # Qty. to Match = QUANTITY (client's custom API), only when
                # the GL_Details QUANTITY differs from the line BC created
                # qty = GL_Details QUANTITY; got = quantity on the BC line
                if l.get("QUANTITY") is not None:
                    qty = float(l["QUANTITY"])
                    got = bc_qty.get(line_id)
                    if got is not None and round(float(got), 5) == round(qty, 5):
                        step(f"qty_{l.get('ID')}", "skipped",
                             f"BC line quantity is already {qty}",
                             inv_id=invoice_id)
                    else:
                        bc_request("PATCH",
                                   f"{custom}/purchaseLineMatches({line_id})",
                                   json_body={"dcgcQtyToMatch": qty}, etag="*",
                                   timeout=cfg.BC_HTTP_TIMEOUT)
                        step(f"qty_{l.get('ID')}", "ok",
                             f"dcgcQtyToMatch = {qty} (BC line had {got})",
                             inv_id=invoice_id)

            elif not line_id:
                # GL variance line; new_line = BC's reply, with the new line's id
                new_line = bc_request("POST", lines_url, json_body={
                    "lineType": "Account",
                    "accountId": accounts[l.get("ID")],
                    "description": str(l.get("GL_DESCR") or "")[:100],
                    "quantity": 1,
                    "unitCost": float(l.get("AMOUNT") or 0),
                }, timeout=cfg.BC_HTTP_TIMEOUT)
                bc_qty[new_line["id"]] = new_line.get("quantity")
                step(key, "ok", f"{new_line['id']} GL {l.get('GLCODE')} "
                     f"amount {l.get('AMOUNT')}", inv_id=invoice_id)
                post_dimensions(api, invoice_id, new_line["id"], l, step)

        # ---- posting description (client's custom API) ------------------
        post_desc = str(header["DESCRIPTION"])
        bc_request("PATCH", f"{custom}/{cfg.CUSTOM_INVOICE_ENTITY}({invoice_id})",
                   json_body={cfg.POSTING_DESC_FIELD: post_desc},
                   etag="*", timeout=cfg.BC_HTTP_TIMEOUT)
        step("posting_description", "ok",
             f"{cfg.POSTING_DESC_FIELD} = {post_desc}")

        # ---- invoice PDF ------------------------------------------------
        attach_invoice_pdf(bc_guid, invoice_id, record_id, step)

        # invoice = the BC invoice as it is now; number = its PINV number
        invoice = bc_get(f"{api}/purchaseInvoices({invoice_id})",
                         timeout=cfg.BC_HTTP_TIMEOUT) or {}
        number = invoice.get("number")

        # ---- post, or leave as a draft ----------------------------------
        # outcome = "posted" or "draft_created", returned in the result
        if cfg.POST_MODE == "post":
            bc_request("POST", f"{api}/purchaseInvoices({invoice_id})"
                       f"/Microsoft.NAV.post", timeout=cfg.BC_HTTP_TIMEOUT)
            step("post_invoice", "ok", number, inv_id=invoice_id, inv_no=number)
            outcome = "posted"
        else:
            step("post_invoice", "skipped",
                 "AP_POST_MODE=draft - a BC user reviews and posts",
                 inv_id=invoice_id, inv_no=number)
            outcome = "draft_created"

        # ERP_POST -> 'P' and the CF audit row
        mark_posted(conn, record_id, number or invoice_id)
        step("erp_post", "ok", f"ERP_POST set to {cfg.DC_ERP_POST_DONE}")

        return {"recordId": record_id, "invoiceType": "PO",
                "status": outcome, "invoiceId": invoice_id,
                "invoiceNumber": number, "lines": len(lines),
                "totalAmountExcludingTax":
                    invoice.get("totalAmountExcludingTax")}

    except Exception as e:
        # any failure: log it, ERP_ERROR, ERP_POST 'N' (or 'E' after the last
        # retry), CF audit row
        return mark_failed(conn, record_id, attempt, e, step)


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

def run_batch(conn, limit=None, record_id=None) -> dict:
    """
    record_id given -> just that one, ignoring STATUS/ERP_POST, for UAT.
    Otherwise everything pending (PO and non-PO), up to the batch size.
    """
    if record_id is not None:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT dc.RECORD_ID, dc.CLIENT_ID, dc.COMPANY_ID, dc.DOC_NO,
                   dc.INVOICE_TYPE, dc.STATUS, dc.ERP_POST
            FROM dbo.AP_DataConversions dc
            WHERE dc.RECORD_ID = %s AND {cfg.company_scope_sql('dc')}
            """,
            (record_id,),
        )
        records = _rows(cur)
        if not records:
            return {"picked": 0,
                    "note": f"RECORD_ID {record_id} not found within "
                            f"CLIENT_ID {cfg.CLIENT_ID} / companies "
                            f"{cfg.COMPANY_IDS}",
                    "results": []}
    else:
        records = pending_records(conn, limit)

    results = []
    for dc in records:
        # One record's failure never stops the others.
        try:
            if str(dc.get("INVOICE_TYPE") or "").strip().upper() == "PO":
                results.append(process_po_record(conn, dc))
            else:
                results.append(process_record(conn, dc))
        except Exception as e:
            log.exception("Record %s failed outside its own error handling",
                          dc.get("RECORD_ID"))
            try:
                conn.rollback()
            except Exception:
                pass
            results.append({"recordId": dc.get("RECORD_ID"),
                            "status": "error", "error": str(e)[:500]})

    return {"picked": len(records), "mode": cfg.POST_MODE, "results": results}
