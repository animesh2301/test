"""
BC -> ContinuFlow tables, loaded directly.

    vendors               -> dbo.Mst_Vendors
    purchaseOrders        -> dbo.AP_DC_PurchaseOrder
                             (+ vendorOrderNumber from the client's custom API)
      + lines             -> dbo.AP_DC_PurchaseOrderLines
    purchaseReceipts      -> dbo.AP_DC_PurchaseReceipts
                             (+ vendorOrderNumber copied from its PO)
      + lines             -> dbo.AP_DC_PurchaseReceiptLines

mst_store, mst_divisions and COA_Master_SAP are master data the client
maintains directly; accounts / dimensionValues land in bc.stg_ tables
(function_app.py), not here.

Three lookups still read landing tables: bc.stg_paymentTerms,
bc.stg_paymentMethods and bc.stg_items.

Rows are UPDATEd in place and INSERTed only when new, so IDENTITY ids never
change and only BC-sourced columns are overwritten. Concurrency is handled
with sp_getapplock on the business key (no table hints, no indexes needed),
and every BC call for a document happens BEFORE its first SQL write, so a
slow BC response never holds a lock on the CF tables.

Everything is scoped to CF_CLIENT_ID / CF_COMPANY_IDS.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone

import uat_config as cfg
from bc_http import BcError, bc_get, bc_get_all

log = logging.getLogger("cf_direct")

SYSTEM_USER = 1

DIRECT_ENTITIES = {
    "vendors", "purchaseOrders", "purchaseOrderLines",
    "purchaseReceipts", "purchaseReceiptLines",
}

BACKFILL_ENTITIES = ("vendors", "purchaseOrders", "purchaseReceipts")

PAYMENT_TERMS_SQL = ("SELECT TOP 1 code, displayName "
                     "FROM bc.stg_paymentTerms WHERE id = %s")

RECEIPT_FIELDS = [
    "buyFromAddressLine1", "buyFromAddressLine2", "buyFromCity",
    "buyFromCountry", "buyFromPostCode", "buyFromState", "currencyCode",
    "dueDate", "invoiceDate", "number", "orderNumber",
    "payToAddressLine1", "payToAddressLine2", "payToCity", "payToContact",
    "payToCountry", "payToName", "payToPostCode", "payToState",
    "payToVendorNumber", "postingDate", "shipToAddressLine1",
    "shipToAddressLine2", "shipToCity", "shipToContact", "shipToCountry",
    "shipToName", "shipToPostCode", "shipToState", "vendorName",
    "vendorNumber",
]

RECEIPT_LINE_FIELDS = [
    "description", "description2", "discountPercent", "documentId",
    "expectedReceiptDate", "lineObjectNumber", "lineType", "quantity",
    "sequence", "taxPercent", "unitCost", "unitOfMeasureCode",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def val(v):
    """Booleans as 'True'/'False', nested objects dropped, as the history."""
    if v is True:
        return "True"
    if v is False:
        return "False"
    if isinstance(v, (dict, list)):
        return None
    return v


def bc_datetime(v):
    """BC ISO timestamp -> naive UTC datetime."""
    if not v:
        return None
    s = re.sub(r"\.\d+", "", str(v)).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        log.warning("Unparseable BC datetime %r", v)
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def concat_ws(sep, *parts):
    """SQL Server CONCAT_WS: skips NULLs, keeps empty strings."""
    return sep.join(str(p) for p in parts if p is not None)


def upsert(cur, table, key: dict, update: dict,
           insert_only: dict | None = None) -> str:
    """
    UPDATE by key; INSERT if nothing matched. `update` is written on insert
    and update, `insert_only` (CREATED_*) only when the row is created.
    Callers lock_key() the record first, which makes this safe against a
    concurrent event for the same record.
    """
    if any(v is None or v == "" for v in key.values()):
        raise ValueError(f"{table}: empty key value in {key}")

    cur.execute(
        f"UPDATE {table} SET "
        + ", ".join(f"[{c}] = %s" for c in update)
        + " WHERE " + " AND ".join(f"[{c}] = %s" for c in key),
        tuple(update.values()) + tuple(key.values()),
    )
    if cur.rowcount and cur.rowcount > 0:
        return "updated"

    row = {**key, **update, **(insert_only or {})}
    cur.execute(
        f"INSERT INTO {table} (" + ", ".join(f"[{c}]" for c in row)
        + ") VALUES (" + ", ".join("%s" for _ in row) + ")",
        tuple(row.values()),
    )
    return "inserted"


def lock_key(cur, table: str, *parts) -> None:
    """
    Serialise writers of ONE record with a named sp_getapplock lock (no
    table, row or index is locked). Released on commit or rollback.
    """
    name = "cf:" + table + ":" + ":".join(str(p) for p in parts)
    if len(name) > 255:                     # sp_getapplock's limit
        name = "cf:" + hashlib.sha256(name.encode()).hexdigest()

    cur.execute(
        """
        DECLARE @r int;
        EXEC @r = sp_getapplock @Resource = %s, @LockMode = 'Exclusive',
                                @LockOwner = 'Transaction',
                                @LockTimeout = 30000;
        SELECT @r;
        """,
        (name,),
    )
    row = cur.fetchone()
    result = row[0] if row else None
    if result is None or result < 0:
        # -1 timeout, -2 cancelled, -3 deadlock victim, -999 error
        raise RuntimeError(f"Could not lock {name} (sp_getapplock={result})")


def delete_stale(cur, table, where, params, id_col, keep_ids) -> int:
    """Delete a document's lines that are no longer in BC."""
    if keep_ids:
        where += f" AND [{id_col}] NOT IN ({', '.join('%s' for _ in keep_ids)})"
        params = list(params) + list(keep_ids)
    cur.execute(f"DELETE FROM {table} WHERE {where}", tuple(params))
    return cur.rowcount or 0


class Context:
    """One event's company, plus a per-event cache for lookups."""

    def __init__(self, conn, bc_guid: str, company_id: int):
        self.conn = conn
        self.bc_guid = bc_guid
        self.client_id = cfg.CLIENT_ID
        self.company_id = company_id
        self.base = f"{cfg.BC_API_BASE}/companies({bc_guid})"
        self._cache: dict = {}

    def one(self, sql, *params):
        """First row of a lookup (cached), or None when the last param is empty."""
        if not params[-1]:
            return None
        key = (sql, params)
        if key not in self._cache:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self._cache[key] = cur.fetchone()
        return self._cache[key]

    def location_code(self, location_id):
        """BC location code for a PO line's locationId (cached)."""
        if not location_id or location_id == "00000000-0000-0000-0000-000000000000":
            return None
        key = ("loc", location_id)
        if key not in self._cache:
            rec = bc_get(f"{self.base}/locations({location_id})",
                         allow_404=True, timeout=cfg.BC_HTTP_TIMEOUT)
            self._cache[key] = (rec or {}).get("code")
        return self._cache[key]


# ---------------------------------------------------------------------------
# 1. Mst_Vendors
# ---------------------------------------------------------------------------

def upsert_vendor(cur, ctx: Context, v: dict) -> str:
    pt = ctx.one(PAYMENT_TERMS_SQL, v.get("paymentTermsId")) or (None, None)
    pm = ctx.one("SELECT TOP 1 code FROM bc.stg_paymentMethods WHERE id = %s",
                 v.get("paymentMethodId"))
    lmdt = bc_datetime(v.get("lastModifiedDateTime"))

    lock_key(cur, "Mst_Vendors", ctx.client_id, ctx.company_id, v.get("number"))

    # BC serialises a blank enum as '_x0020_'
    blocked = str(v.get("blocked") or "").strip()
    is_active = (1 if blocked in ("", "_x0020_", "Payment")
                 else 0 if blocked == "All" else None)

    return upsert(
        cur, "dbo.Mst_Vendors",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "BP_CODE": v.get("number")},
        update={
            "BP_NAME": v.get("displayName"),
            "FOREIGN_NAME": v.get("displayName"),
            "PAYMENT_TERMS_CODE": pt[0],
            "BP_CURRENCY": v.get("currencyCode"),
            "PAYMENT_METHOD_CODE": pm[0] if pm else None,
            "UPDATED_BY": SYSTEM_USER,
            "UPDATED_DATE": lmdt,
            "Address": v.get("addressLine1"),
            "City": v.get("city"),
            "State": v.get("state"),
            "Zip": v.get("postalCode"),
            "isActive": is_active,
            "Payment_Terms": pt[1],
        },
        insert_only={"CREATED_BY": SYSTEM_USER, "CREATED_DATE": lmdt},
    )


# ---------------------------------------------------------------------------
# 2. AP_DC_PurchaseOrder + AP_DC_PurchaseOrderLines
# ---------------------------------------------------------------------------

def vendor_order_number(ctx: Context, po_id: str):
    """
    vendorOrderNumber from the client's custom purchaseOrders API. None -
    column left as it is - when the custom API isn't set, the PO isn't
    there, or the call fails, so this API never stops the PO from syncing.
    """
    if not cfg.CUSTOM_API_BASE:
        return None
    try:
        rec = bc_get(f"{cfg.CUSTOM_API_BASE}/companies({ctx.bc_guid})"
                     f"/purchaseOrders({po_id})",
                     allow_404=True, timeout=cfg.BC_HTTP_TIMEOUT) or {}
    except BcError as e:
        log.warning("vendorOrderNumber not read for PO %s: %s %s",
                    po_id, e, e.body or "")
        return None
    if "vendorOrderNumber" not in rec:
        log.warning("Custom purchaseOrders API returned no vendorOrderNumber "
                    "for PO %s", po_id)
        return None
    return rec["vendorOrderNumber"]


def upsert_purchase_order(cur, ctx: Context, po: dict,
                          lines: list[dict]) -> str:
    # BC calls first: every line's location code, before any lock is taken.
    for pl in lines:
        ctx.location_code(pl.get("locationId"))

    # One lock for the whole document, lines included.
    lock_key(cur, "AP_DC_PurchaseOrder", ctx.company_id, po.get("id"))

    pt = ctx.one(PAYMENT_TERMS_SQL, po.get("paymentTermsId")) or (None, None)
    pm = ctx.one("SELECT TOP 1 PAYMENT_METHOD_CODE FROM dbo.Mst_Vendors "
                 "WHERE CLIENT_ID = %s AND COMPANY_ID = %s AND BP_CODE = %s",
                 ctx.client_id, ctx.company_id, po.get("vendorNumber"))
    lmdt = bc_datetime(po.get("lastModifiedDateTime"))

    update = {
        "POnumber": po.get("number"),
        "vendorNumber": po.get("vendorNumber"),
        "vendorName": po.get("vendorName"),
        "vendorAddressCode": concat_ws(
            ", ", po.get("buyFromAddressLine1"),
            po.get("buyFromAddressLine2"), po.get("buyFromCity"),
            po.get("buyFromState"), po.get("buyFromPostCode"),
            po.get("buyFromCountry")),
        "BuyerPurchaserCode": po.get("purchaser"),
        "BuyerPurchaserName": po.get("shipToName"),
        "PODate": po.get("orderDate"),
        "postingDate": po.get("postingDate"),
        "requestedReceiptDate": po.get("requestedReceiptDate"),
        "currencyCode": po.get("currencyCode"),
        "PaymentTerms": pt[0],
        "PaymentMethod": pm[0] if pm else None,
        "ShiptoLocation": concat_ws(
            ", ", po.get("shipToAddressLine1"),
            po.get("shipToAddressLine2"), po.get("shipToCity"),
            po.get("shipToState"), po.get("shipToPostCode"),
            po.get("shipToCountry")),
        "BilltoLocation": concat_ws(
            ", ", po.get("payToAddressLine1"),
            po.get("payToAddressLine2"), po.get("payToCity"),
            po.get("payToState"), po.get("payToPostCode"),
            po.get("payToCountry")),
        "Dimension1": po.get("shortcutDimension1Code"),
        "Dimension2": po.get("shortcutDimension2Code"),
        "TaxAmount": val(po.get("totalTaxAmount")),
        "DiscountAmount": val(po.get("discountAmount")),
        "SubtotalAmount": val(po.get("totalAmountExcludingTax")),
        "TotalAmount": val(po.get("totalAmountIncludingTax")),
        "DocumentStatus": po.get("status"),
        "ModifiedBy": SYSTEM_USER,
        "ModifiedDate": lmdt,
    }
    # Set by write_record from the custom API; None = leave the column as is
    if po.get("vendorOrderNumber") is not None:
        update["vendorOrderNumber"] = po["vendorOrderNumber"]

    result = upsert(
        cur, "dbo.AP_DC_PurchaseOrder",
        # this table has CompanyCode, no CLIENT_ID
        key={"CompanyCode": ctx.company_id, "POInternalID": po.get("id")},
        update=update,
        insert_only={"CreatedBy": SYSTEM_USER, "CreatedDate": lmdt},
    )

    for pl in lines:
        item = ctx.one("SELECT TOP 1 number, type, itemCategoryCode "
                       "FROM bc.stg_items WHERE id = %s",
                       pl.get("itemId")) or (None, None, None)
        upsert(
            cur, "dbo.AP_DC_PurchaseOrderLines",
            # the doc maps the LINE's BC id into POInternalID
            key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
                 "POInternalID": pl.get("id")},
            update={
                "PONumber": po.get("number"),
                "LineNumber": pl.get("lineObjectNumber"),
                "ItemNumber": item[0],
                "ItemDescription": pl.get("description"),
                "ExtendedDescription": pl.get("description2"),
                "ItemType": item[1],
                "Category": item[2],
                "UOM": pl.get("unitOfMeasureCode"),
                "OrderedQuantity": val(pl.get("quantity")),
                "ReceivedQuantity": val(pl.get("receivedQuantity")),
                "InvoicedQuantity": val(pl.get("invoicedQuantity")),
                "UnitCost": val(pl.get("directUnitCost")),
                "LineDiscountPercent": val(pl.get("discountPercent")),
                "LineDiscountAmount": val(pl.get("discountAmount")),
                "taxCode": pl.get("taxCode"),
                "TaxAmount": val(pl.get("totalTaxAmount")),
                "LocationCode": ctx.location_code(pl.get("locationId")),
                # the PO's dimension codes, exactly as the doc does
                "Dimension1": po.get("shortcutDimension1Code"),
                "Dimension2": po.get("shortcutDimension2Code"),
                "expectedReceiptDate": pl.get("expectedReceiptDate"),
            },
        )

    # A line removed from the PO in BC is removed here too.
    removed = delete_stale(
        cur, "dbo.AP_DC_PurchaseOrderLines",
        "CLIENT_ID = %s AND COMPANY_ID = %s AND PONumber = %s",
        [ctx.client_id, ctx.company_id, po.get("number")],
        "POInternalID", [pl["id"] for pl in lines])

    return f"{result}, {len(lines)} line(s), {removed} stale line(s) removed"


# ---------------------------------------------------------------------------
# 3. AP_DC_PurchaseReceipts + AP_DC_PurchaseReceiptLines
# ---------------------------------------------------------------------------

def upsert_purchase_receipt(cur, ctx: Context, pr: dict,
                            lines: list[dict]) -> str:
    # One lock for the whole document, lines included.
    lock_key(cur, "AP_DC_PurchaseReceipts", ctx.client_id, ctx.company_id,
             pr.get("id"))

    update = {f: val(pr.get(f)) for f in RECEIPT_FIELDS}
    update["lastModifiedDateTime"] = bc_datetime(pr.get("lastModifiedDateTime"))

    # vendorOrderNumber comes from the receipt's PO. Left as it is when the
    # PO row isn't there (e.g. already removed after invoicing).
    cur.execute(
        "SELECT TOP 1 vendorOrderNumber FROM dbo.AP_DC_PurchaseOrder "
        "WHERE CompanyCode = %s AND POnumber = %s "
        "AND vendorOrderNumber IS NOT NULL",
        (ctx.company_id, pr.get("orderNumber")),
    )
    row = cur.fetchone()
    if row:
        update["vendorOrderNumber"] = row[0]

    result = upsert(
        cur, "dbo.AP_DC_PurchaseReceipts",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "id": pr.get("id")},
        update=update,
    )

    for rl in lines:
        dims = {d["code"]: d["valueCode"]
                for d in rl.get("dimensionSetLines") or []}
        line_update = {f: val(rl.get(f)) for f in RECEIPT_LINE_FIELDS}
        line_update["Division"] = dims.get(cfg.DIVISION_DIMENSION_CODE)
        line_update["Cost_Center"] = dims.get(cfg.COST_CENTER_DIMENSION_CODE)
        upsert(
            cur, "dbo.AP_DC_PurchaseReceiptLines",
            key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
                 "id": rl.get("id")},
            update=line_update,
        )

    removed = delete_stale(
        cur, "dbo.AP_DC_PurchaseReceiptLines",
        "CLIENT_ID = %s AND COMPANY_ID = %s AND documentId = %s",
        [ctx.client_id, ctx.company_id, pr.get("id")],
        "id", [rl["id"] for rl in lines])

    return f"{result}, {len(lines)} line(s), {removed} stale line(s) removed"


# ---------------------------------------------------------------------------
# Fetch + write one record
# ---------------------------------------------------------------------------

def write_record(cur, ctx: Context, entity: str, rec: dict) -> str:
    """Write an already-fetched BC record (used by events and backfill)."""
    if entity == "vendors":
        return upsert_vendor(cur, ctx, rec)
    if entity == "purchaseOrders":
        lines = bc_get_all(f"{ctx.base}/purchaseOrders({rec['id']})"
                           f"/purchaseOrderLines",
                           allow_404=True, timeout=cfg.BC_HTTP_TIMEOUT)
        rec = {**rec, "vendorOrderNumber": vendor_order_number(ctx, rec["id"])}
        return upsert_purchase_order(cur, ctx, rec, lines)
    if entity == "purchaseReceipts":
        lines = bc_get_all(f"{ctx.base}/purchaseReceipts({rec['id']})"
                           f"/purchaseReceiptLines?$expand=dimensionSetLines",
                           allow_404=True, timeout=cfg.BC_HTTP_TIMEOUT)
        return upsert_purchase_receipt(cur, ctx, rec, lines)
    raise ValueError(f"cf_direct cannot write entity '{entity}'")


def sync_record(cur, ctx: Context, entity: str, record_id: str) -> str:
    # A line event re-syncs its whole document (PO lines need the header,
    # and a full re-read also removes lines deleted in BC).
    if entity in ("purchaseOrderLines", "purchaseReceiptLines"):
        line = bc_get(f"{ctx.base}/{entity}({record_id})", allow_404=True,
                      timeout=cfg.BC_HTTP_TIMEOUT)
        if line is None:
            return "line no longer in BC; its document's next event tidies up"
        entity = ("purchaseOrders" if entity == "purchaseOrderLines"
                  else "purchaseReceipts")
        record_id = line["documentId"]

    rec = bc_get(f"{ctx.base}/{entity}({record_id})", allow_404=True,
                 timeout=cfg.BC_HTTP_TIMEOUT)
    if rec is None:
        return "record no longer in BC"
    return write_record(cur, ctx, entity, rec)


# ---------------------------------------------------------------------------
# Deletes - purchaseOrders and purchaseOrderLines only. Vendors are blocked
# in BC rather than deleted; other deletes are logged, not applied.
# ---------------------------------------------------------------------------

def delete_purchase_order(cur, ctx: Context, bc_id: str) -> str:
    # Same lock as upsert_purchase_order
    lock_key(cur, "AP_DC_PurchaseOrder", ctx.company_id, bc_id)

    cur.execute(
        """
        DELETE FROM dbo.AP_DC_PurchaseOrderLines
        WHERE CLIENT_ID = %s AND COMPANY_ID = %s
          AND PONumber IN (SELECT POnumber FROM dbo.AP_DC_PurchaseOrder
                           WHERE CompanyCode = %s AND POInternalID = %s)
        """,
        (ctx.client_id, ctx.company_id, ctx.company_id, bc_id),
    )
    lines = cur.rowcount or 0

    cur.execute("DELETE FROM dbo.AP_DC_PurchaseOrder "
                "WHERE CompanyCode = %s AND POInternalID = %s",
                (ctx.company_id, bc_id))
    headers = cur.rowcount or 0
    if not headers:
        return "PO not in AP_DC_PurchaseOrder; nothing to delete"
    return f"PO {bc_id} deleted ({headers} header, {lines} line(s))"


def delete_purchase_order_line(cur, ctx: Context, bc_id: str) -> str:
    # Lock the parent PO, as an update of that PO would.
    cur.execute(
        "SELECT TOP 1 h.POInternalID "
        "FROM dbo.AP_DC_PurchaseOrderLines l "
        "JOIN dbo.AP_DC_PurchaseOrder h "
        "  ON h.CompanyCode = l.COMPANY_ID AND h.POnumber = l.PONumber "
        "WHERE l.CLIENT_ID = %s AND l.COMPANY_ID = %s AND l.POInternalID = %s",
        (ctx.client_id, ctx.company_id, bc_id),
    )
    row = cur.fetchone()
    if row and row[0]:
        lock_key(cur, "AP_DC_PurchaseOrder", ctx.company_id, row[0])

    cur.execute(
        "DELETE FROM dbo.AP_DC_PurchaseOrderLines "
        "WHERE CLIENT_ID = %s AND COMPANY_ID = %s AND POInternalID = %s",
        (ctx.client_id, ctx.company_id, bc_id),
    )
    return f"PO line deleted ({cur.rowcount or 0} row(s))"


DELETE_HANDLERS = {
    "purchaseOrders": delete_purchase_order,
    "purchaseOrderLines": delete_purchase_order_line,
}


def handle_event(conn, change_type: str, entity: str, bc_guid: str,
                 record_id: str) -> str:
    company_id = cfg.cf_company_id(bc_guid)
    if company_id is None or company_id not in cfg.COMPANY_IDS:
        log.info("Skipping %s %s: BC company %s is not in CF_BC_COMPANY_MAP "
                 "or not in CF_COMPANY_IDS", entity, record_id, bc_guid)
        return "out of scope"

    handler = DELETE_HANDLERS.get(entity) if change_type == "deleted" else None
    if change_type == "deleted" and handler is None:
        log.warning("BC deleted %s %s (company %s) - not applied; delete "
                    "handling for %s is pending with the CF team",
                    entity, record_id, company_id, entity)
        return "delete not applied"

    ctx = Context(conn, bc_guid, company_id)
    cur = conn.cursor()
    try:
        result = (handler(cur, ctx, record_id) if handler
                  else sync_record(cur, ctx, entity, record_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    log.info("cf_direct %s %s (company %s): %s",
             entity, record_id, company_id, result)
    return result


# ---------------------------------------------------------------------------
# Backfill - re-read from BC and upsert again
# ---------------------------------------------------------------------------

def backfill(conn, entity: str, company_ids=None, modified_since=None,
             top=None) -> dict:
    """
    Re-pull an entity from BC and upsert every record, after missed or
    dead-lettered events. modified_since ('2026-10-01') limits it to records
    changed since then - use it for the document entities.
    """
    if entity not in BACKFILL_ENTITIES:
        raise ValueError(f"entity must be one of {BACKFILL_ENTITIES}")

    summary = {"entity": entity, "companies": {}}

    for company_id in company_ids or cfg.COMPANY_IDS:
        ctx = Context(conn, cfg.bc_company_guid(company_id), company_id)

        query = []
        if modified_since:
            query.append(f"$filter=lastModifiedDateTime gt "
                         f"{modified_since}T00:00:00Z")
        if top:
            query.append(f"$top={int(top)}")
        records = bc_get_all(f"{ctx.base}/{entity}"
                             + ("?" + "&".join(query) if query else ""),
                             timeout=cfg.BC_HTTP_TIMEOUT)

        counts = {"read": len(records), "inserted": 0, "updated": 0,
                  "skipped": 0, "failed": 0}
        errors = []

        for rec in records:
            cur = conn.cursor()
            try:
                result = write_record(cur, ctx, entity, rec)
                conn.commit()
                kind = result.split(",")[0]
                counts[kind if kind in ("inserted", "updated")
                       else "skipped"] += 1
            except Exception as e:
                conn.rollback()
                counts["failed"] += 1
                if len(errors) < 20:
                    errors.append({"id": rec.get("id"),
                                   "number": rec.get("number")
                                   or rec.get("code"),
                                   "error": str(e)[:300]})

        summary["companies"][company_id] = {**counts, "errors": errors}

    return summary
