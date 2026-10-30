"""
BC -> ContinuFlow tables, loaded directly.

    vendors               -> dbo.Mst_Vendors               -                         
    locations             -> dbo.mst_store                 - 
    accounts (Expense)    -> dbo.COA_Master_SAP            -  
    purchaseOrders        -> dbo.AP_DC_PurchaseOrder       -  
      + lines             -> dbo.AP_DC_PurchaseOrderLines  - 
    purchaseReceipts      -> dbo.AP_DC_PurchaseReceipts    - 
      + lines             -> dbo.AP_DC_PurchaseReceiptLines-  

No bc.stg_* copy is written for these. The mappings are the CF team's
SELECTs from the doc, column for column, with three lookups that still read
landing tables because their entities are not in the dbo schema:

    bc.stg_paymentTerms    PAYMENT_TERMS_CODE / Payment_Terms / PaymentTerms
    bc.stg_paymentMethods  PAYMENT_METHOD_CODE
    bc.stg_items           ItemNumber / ItemType / Category on PO lines

Rows are UPDATEd in place and INSERTed only when new. Two reasons:
  * IDENTITY ids never change. Mst_Store.PROFIT_CENTER_ID is referenced by
    AP_DataConversions and AP_DC_GL_Details; delete-and-reinsert would
    hand every store a new id on every BC edit.
  * Only columns sourced from BC are overwritten on update. Anything the CF
    team fills into the doc's NULL placeholder columns is left alone.

Everything is scoped to CF_CLIENT_ID / CF_COMPANY_IDS. A BC company not in
CF_BC_COMPANY_MAP is skipped.

No indexes are required on the dbo tables. They belong to the CF team and
hold every client's data, so this module is written to behave well without
any index beyond what they already have:
  * concurrency is controlled with sp_getapplock on the business key, not
    with table hints - so nothing ever locks a range of their table;
  * every BC call for a document happens BEFORE its first SQL write, so a
    slow or throttled BC response never holds a lock on their tables.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone

import uat_config as cfg
from bc_http import bc_get, bc_get_all

log = logging.getLogger("cf_direct")

SYSTEM_USER = 1

DIRECT_ENTITIES = {
    "vendors", "dimensionValues", "accounts",
    "purchaseOrders", "purchaseOrderLines",
    "purchaseReceipts", "purchaseReceiptLines",
}

BACKFILL_ENTITIES = ("vendors", "dimensionValues", "accounts",
                     "purchaseOrders", "purchaseReceipts")


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

class SqlExpr(str):
    """A value written into the statement as SQL, not passed as a parameter."""


# The same expression the CF team's initial load used for Mst_Store, so new
# rows carry the same timestamp convention as the rows already there.
NOW_EASTERN = SqlExpr(
    "CAST(SYSUTCDATETIME() AT TIME ZONE 'Eastern Standard Time' "
    "AS smalldatetime)"
)


def val(v):
    """
    Same coercion the bc.stg_* loader used for the historical data: booleans
    as 'True'/'False', nested objects dropped, numbers passed through for SQL
    Server to convert - so new rows read the same as the rows already loaded.
    """
    if v is True:
        return "True"
    if v is False:
        return "False"
    if isinstance(v, (dict, list)):
        return None
    return v


def bc_datetime(v):
    """BC ISO timestamp -> naive UTC datetime, for datetime2/smalldatetime."""
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
    """
    SQL Server CONCAT_WS: skips NULLs but keeps empty strings, and returns ''
    when everything is NULL. Matched exactly so addresses built here look like
    the ones the doc's SELECTs produced.
    """
    return sep.join(str(p) for p in parts if p is not None)


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _render(columns: dict):
    frags, params = [], []
    for col, v in columns.items():
        if isinstance(v, SqlExpr):
            frags.append((col, str(v)))
        else:
            frags.append((col, "%s"))
            params.append(v)
    return frags, params


def upsert(cur, table, key: dict, update: dict,
           insert_only: dict | None = None) -> str:
    """
    UPDATE by key; INSERT if nothing matched.

      update       written on insert AND on update - BC-sourced columns, plus
                   UPDATED_BY / UPDATED_DATE
      insert_only  written only when the row is created - CREATED_BY /
                   CREATED_DATE and the doc's constants

    So on create, CREATED_DATE and UPDATED_DATE get the same value; after
    that CREATED_DATE never changes and only UPDATED_DATE moves.

    Callers take lock_key() on the record first, which is what makes
    update-then-insert safe against a concurrent event for the same record.
    No table hints here: on a table with no index on the key, a HOLDLOCK hint
    would range-lock the whole table until commit.
    """
    if any(v is None or v == "" for v in key.values()):
        raise ValueError(f"{table}: empty key value in {key}")

    set_frags, set_params = _render(update)
    where = " AND ".join(f"[{c}] = %s" for c in key)

    cur.execute(
        f"UPDATE {table} SET "
        + ", ".join(f"[{c}] = {p}" for c, p in set_frags)
        + f" WHERE {where}",
        tuple(set_params) + tuple(key.values()),
    )
    if cur.rowcount and cur.rowcount > 0:
        return "updated"

    ins_frags, ins_params = _render({**key, **update, **(insert_only or {})})
    cur.execute(
        f"INSERT INTO {table} ("
        + ", ".join(f"[{c}]" for c, _ in ins_frags)
        + ") VALUES ("
        + ", ".join(p for _, p in ins_frags)
        + ")",
        tuple(ins_params),
    )
    return "inserted"


def lock_key(cur, table: str, *parts) -> None:
    """
    Serialise writers of ONE record, without locking anything in the table.

    Two events can reach the same dbo row at the same time - a receipt and
    one of its lines landing on different Event Hub partitions, or a backfill
    running while webhooks are processed. Without this, both could UPDATE
    (0 rows), then both INSERT, leaving a duplicate.

    sp_getapplock takes a named lock that exists only in SQL Server's lock
    manager - no table, row or index is touched - so it needs no index and
    blocks no one but another writer of this exact record. Owned by the
    transaction, so it is released automatically on commit or rollback.
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


def _not_in(column, values):
    return f"[{column}] NOT IN ({', '.join('%s' for _ in values)})"


# ---------------------------------------------------------------------------
# Lookups - cached per event, so a 40-line PO doesn't repeat 40 queries
# ---------------------------------------------------------------------------

class Context:
    def __init__(self, conn, bc_guid: str, company_id: int):
        self.conn = conn
        self.bc_guid = bc_guid
        self.client_id = cfg.CLIENT_ID
        self.company_id = company_id
        self.base = f"{cfg.BC_API_BASE}/companies({bc_guid})"
        self._cache: dict = {}

    def _one(self, cache_key, sql, params):
        if cache_key not in self._cache:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self._cache[cache_key] = cur.fetchone()
        return self._cache[cache_key]

    def payment_terms(self, terms_id):
        """(code, displayName) from bc.stg_paymentTerms."""
        if not terms_id:
            return None, None
        row = self._one(("pt", terms_id),
                        "SELECT TOP 1 code, displayName "
                        "FROM bc.stg_paymentTerms WHERE id = %s",
                        (terms_id,))
        return (row[0], row[1]) if row else (None, None)

    def payment_method_code(self, method_id):
        if not method_id:
            return None
        row = self._one(("pm", method_id),
                        "SELECT TOP 1 code FROM bc.stg_paymentMethods "
                        "WHERE id = %s",
                        (method_id,))
        return row[0] if row else None

    def item(self, item_id):
        """(number, type, itemCategoryCode) from bc.stg_items."""
        if not item_id:
            return None, None, None
        row = self._one(("item", item_id),
                        "SELECT TOP 1 number, type, itemCategoryCode "
                        "FROM bc.stg_items WHERE id = %s",
                        (item_id,))
        return (row[0], row[1], row[2]) if row else (None, None, None)

    def vendor_payment_method(self, vendor_number):
        """
        The doc reaches the PO's PaymentMethod through
        stg_vendors.paymentMethodId -> stg_paymentMethods.code. Vendors now
        load straight into Mst_Vendors, whose PAYMENT_METHOD_CODE already
        holds that same value, so it is read from there.
        """
        if not vendor_number:
            return None
        row = self._one(("vpm", vendor_number),
                        "SELECT TOP 1 PAYMENT_METHOD_CODE FROM dbo.Mst_Vendors "
                        "WHERE CLIENT_ID = %s AND COMPANY_ID = %s "
                        "AND BP_CODE = %s",
                        (self.client_id, self.company_id, vendor_number))
        return row[0] if row else None

    def branch_id(self):
        """
        The doc LEFT JOINs mst_branch on company_id; if a company had several
        branches that would create one store row per branch. One row per
        store code is kept here, using the lowest branch_id.
        """
        row = self._one(("branch",),
                        "SELECT TOP 1 branch_id FROM dbo.mst_branch "
                        "WHERE company_id = %s ORDER BY branch_id",
                        (self.company_id,))
        return row[0] if row else None

    def dimension(self, dimension_id):
        """(code, displayName) of a BC dimension, read from BC and cached."""
        if not dimension_id:
            return None, None
        key = ("dim", dimension_id)
        if key not in self._cache:
            rec = bc_get(f"{self.base}/dimensions({dimension_id})",
                         allow_404=True, timeout=cfg.BC_HTTP_TIMEOUT) or {}
            self._cache[key] = (rec.get("code"), rec.get("displayName"))
        return self._cache[key]

    def is_division(self, dimension_id) -> bool:
        """The CF mapping keeps values WHERE dimension displayName = 'DIVISION'."""
        code, name = self.dimension(dimension_id)
        want = cfg.DIVISION_DIMENSION_NAME.strip().upper()
        return (str(name or "").strip().upper() == want
                or str(code or "").strip().upper() == want)

    def location_code(self, location_id):
        """
        PO lines carry a BC locationId. Locations now load straight into
        mst_store, which stores the code but not the BC id, so the code is
        read from BC. Cached per event; most lines on a PO share a location.
        """
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
    pt_code, pt_name = ctx.payment_terms(v.get("paymentTermsId"))
    pm_code = ctx.payment_method_code(v.get("paymentMethodId"))
    lmdt = bc_datetime(v.get("lastModifiedDateTime"))

    lock_key(cur, "Mst_Vendors", ctx.client_id, ctx.company_id, v.get("number"))

    # BC serialises a blank enum as '_x0020_'
    blocked = str(v.get("blocked") or "").strip()
    if blocked in ("", "_x0020_", "Payment"):
        is_active = 1
    elif blocked == "All":
        is_active = 0
    else:
        is_active = None

    return upsert(
        cur, "dbo.Mst_Vendors",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "BP_CODE": v.get("number")},
        update={
            "BP_NAME": v.get("displayName"),
            "FOREIGN_NAME": v.get("displayName"),
            "PAYMENT_TERMS_CODE": pt_code,
            "BP_CURRENCY": v.get("currencyCode"),
            "PAYMENT_METHOD_CODE": pm_code,
            "UPDATED_BY": SYSTEM_USER,
            "UPDATED_DATE": lmdt,
            "Address": v.get("addressLine1"),
            "City": v.get("city"),
            "State": v.get("state"),
            "Zip": v.get("postalCode"),
            "isActive": is_active,
            "Payment_Terms": pt_name,
        },
        insert_only={"CREATED_BY": SYSTEM_USER, "CREATED_DATE": lmdt},
    )


# ---------------------------------------------------------------------------
# 2. mst_store
# ---------------------------------------------------------------------------

def upsert_division(cur, ctx: Context, dv: dict) -> str:
    """
    BC dimension value of the DIVISION dimension -> dbo.mst_divisions and
    dbo.mst_store, following the CF team's two SELECTs:

      mst_divisions  CLIENT_ID, COMPANY_ID, ID = dv.id,
                     DivisionValue = dv.code, DivisionName = dv.displayName
      mst_store      PROFIT_CENTER_CODE = code, PROFIT_CENTER_NAME = name,
                     INDUSTRY_ID 6, BRANCH_ID, IS_ACTIVE 1, CREATED_BY 1;
                     address / contact columns NULL (never written)

    Values of any other dimension are skipped.
    """
    # BC call first - before any lock is taken
    if not ctx.is_division(dv.get("dimensionId")):
        return "skipped: not a DIVISION dimension value"

    code = dv.get("code")
    name = dv.get("displayName") or code
    lock_key(cur, "mst_store", ctx.client_id, ctx.company_id, code)

    upsert(
        cur, "dbo.mst_divisions",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "ID": dv.get("id")},
        update={"DivisionValue": code, "DivisionName": dv.get("displayName")},
    )

    return upsert(
        cur, "dbo.mst_store",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "PROFIT_CENTER_CODE": code},
        update={
            # NOT NULL in the DDL; fall back to the code rather than fail
            "PROFIT_CENTER_NAME": name,
            # Set on create too, so a new row has DT_UPDATED_ON equal to
            # DT_CREATED_ON; after that only these two move.
            "UPDATED_BY": SYSTEM_USER,
            "DT_UPDATED_ON": NOW_EASTERN,
        },
        insert_only={
            "INDUSTRY_ID": 6,
            "BRANCH_ID": ctx.branch_id(),
            "IS_ACTIVE": 1,
            "CREATED_BY": SYSTEM_USER,
            "DT_CREATED_ON": NOW_EASTERN,
        },
    )


# ---------------------------------------------------------------------------
# 3. COA_Master_SAP  (Expense accounts only, as in the doc)
# ---------------------------------------------------------------------------

def upsert_account(cur, ctx: Context, a: dict) -> str:
    if (a.get("category") or "") != "Expense":
        return "skipped: not an Expense account"

    lock_key(cur, "COA_Master_SAP", ctx.client_id, ctx.company_id, a.get("id"))
    lmdt = bc_datetime(a.get("lastModifiedDateTime"))

    # As in the doc: blocked FALSE -> 'T', TRUE -> 'F'
    blocked = a.get("blocked")
    if blocked in (False, "False", "FALSE", "false"):
        inactive = "T"
    elif blocked in (True, "True", "TRUE", "true"):
        inactive = "F"
    else:
        inactive = None

    return upsert(
        cur, "dbo.COA_Master_SAP",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "ACCOUNT_NUMBER": a.get("id")},
        update={
            "ACCOUNT_CODE": a.get("number"),
            "ACCOUNT_NAME": a.get("displayName"),
            "SEGMENT_0": a.get("number"),
            "INACTIVE": inactive,
            "UPDATED_BY": SYSTEM_USER,
            "UPDATED_DATE": lmdt,
        },
        insert_only={"CREATED_BY": SYSTEM_USER, "CREATED_DATE": lmdt},
    )


# ---------------------------------------------------------------------------
# 4. AP_DC_PurchaseOrder + AP_DC_PurchaseOrderLines
# ---------------------------------------------------------------------------

def upsert_purchase_order(cur, ctx: Context, po: dict,
                          lines: list[dict]) -> str:
    # Resolve every line's location code from BC now, before the first
    # write. Once lock_key / UPDATE run the transaction holds locks, and a
    # slow or throttled BC call must not keep them held. Results are cached,
    # so upsert_po_line below reads them without calling BC.
    for pl in lines:
        ctx.location_code(pl.get("locationId"))

    # One lock for the whole document: lines are only ever written as part of
    # their PO, so this also serialises everything below.
    lock_key(cur, "AP_DC_PurchaseOrder", ctx.company_id, po.get("id"))

    pt_code, _ = ctx.payment_terms(po.get("paymentTermsId"))
    lmdt = bc_datetime(po.get("lastModifiedDateTime"))

    result = upsert(
        cur, "dbo.AP_DC_PurchaseOrder",
        # this table has CompanyCode, no CLIENT_ID
        key={"CompanyCode": ctx.company_id, "POInternalID": po.get("id")},
        update={
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
            "PaymentTerms": pt_code,
            "PaymentMethod": ctx.vendor_payment_method(po.get("vendorNumber")),
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
        },
        insert_only={"CreatedBy": SYSTEM_USER, "CreatedDate": lmdt},
    )

    seen = []
    for pl in lines:
        upsert_po_line(cur, ctx, po, pl)
        seen.append(pl["id"])

    # A line removed from the PO in BC is removed here too; otherwise
    # ContinuFlow would match invoices against a line that no longer exists.
    where = ("CLIENT_ID = %s AND COMPANY_ID = %s AND PONumber = %s")
    params = [ctx.client_id, ctx.company_id, po.get("number")]
    if seen:
        where += " AND " + _not_in("POInternalID", seen)
        params += seen
    cur.execute(f"DELETE FROM dbo.AP_DC_PurchaseOrderLines WHERE {where}",
                tuple(params))
    removed = cur.rowcount or 0

    return f"{result}, {len(seen)} line(s), {removed} stale line(s) removed"


def upsert_po_line(cur, ctx: Context, po: dict, pl: dict) -> str:
    item_number, item_type, item_category = ctx.item(pl.get("itemId"))

    return upsert(
        cur, "dbo.AP_DC_PurchaseOrderLines",
        # the doc maps the LINE's BC id into POInternalID
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "POInternalID": pl.get("id")},
        update={
            "PONumber": po.get("number"),
            "LineNumber": pl.get("lineObjectNumber"),
            "ItemNumber": item_number,
            "ItemDescription": pl.get("description"),
            "ExtendedDescription": pl.get("description2"),
            "ItemType": item_type,
            "Category": item_category,
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
            # int columns in the DDL, filled with the PO's dimension codes
            # exactly as the doc does
            "Dimension1": po.get("shortcutDimension1Code"),
            "Dimension2": po.get("shortcutDimension2Code"),
            "expectedReceiptDate": pl.get("expectedReceiptDate"),
        },
    )


# ---------------------------------------------------------------------------
# 5. AP_DC_PurchaseReceipts + AP_DC_PurchaseReceiptLines
# ---------------------------------------------------------------------------

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


def upsert_purchase_receipt(cur, ctx: Context, pr: dict,
                            lines: list[dict]) -> str:
    # One lock for the whole document, lines included.
    lock_key(cur, "AP_DC_PurchaseReceipts", ctx.client_id, ctx.company_id,
             pr.get("id"))

    update = {f: val(pr.get(f)) for f in RECEIPT_FIELDS}
    update["lastModifiedDateTime"] = bc_datetime(pr.get("lastModifiedDateTime"))

    result = upsert(
        cur, "dbo.AP_DC_PurchaseReceipts",
        key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
             "id": pr.get("id")},
        update=update,
    )

    seen = []
    for rl in lines:
        dims = {d["code"]: d["valueCode"]
                for d in rl.get("dimensionSetLines") or []}
        update = {f: val(rl.get(f)) for f in RECEIPT_LINE_FIELDS}
        update["Division"] = dims.get(cfg.DIVISION_DIMENSION_CODE)
        update["Cost_Center"] = dims.get(cfg.COST_CENTER_DIMENSION_CODE)
        upsert(
            cur, "dbo.AP_DC_PurchaseReceiptLines",
            key={"CLIENT_ID": ctx.client_id, "COMPANY_ID": ctx.company_id,
                 "id": rl.get("id")},
            update=update,
        )
        seen.append(rl["id"])

    where = "CLIENT_ID = %s AND COMPANY_ID = %s AND documentId = %s"
    params = [ctx.client_id, ctx.company_id, pr.get("id")]
    if seen:
        where += " AND " + _not_in("id", seen)
        params += seen
    cur.execute(f"DELETE FROM dbo.AP_DC_PurchaseReceiptLines WHERE {where}",
                tuple(params))
    removed = cur.rowcount or 0

    return f"{result}, {len(seen)} line(s), {removed} stale line(s) removed"


# ---------------------------------------------------------------------------
# Fetch + write one record
# ---------------------------------------------------------------------------

def _get(ctx, path):
    return bc_get(f"{ctx.base}/{path}", allow_404=True,
                  timeout=cfg.BC_HTTP_TIMEOUT)


def _lines(ctx, path):
    return bc_get_all(f"{ctx.base}/{path}", allow_404=True,
                      timeout=cfg.BC_HTTP_TIMEOUT)


def write_record(cur, ctx: Context, entity: str, rec: dict) -> str:
    """Write an already-fetched BC record (used by events and backfill)."""
    if entity == "vendors":
        return upsert_vendor(cur, ctx, rec)
    if entity == "dimensionValues":
        return upsert_division(cur, ctx, rec)
    if entity == "accounts":
        return upsert_account(cur, ctx, rec)
    if entity == "purchaseOrders":
        lines = _lines(ctx, f"purchaseOrders({rec['id']})/purchaseOrderLines")
        return upsert_purchase_order(cur, ctx, rec, lines)
    if entity == "purchaseReceipts":
        lines = _lines(ctx,
                       f"purchaseReceipts({rec['id']})/purchaseReceiptLines"
                       f"?$expand=dimensionSetLines")
        return upsert_purchase_receipt(cur, ctx, rec, lines)
    raise ValueError(f"cf_direct cannot write entity '{entity}'")


def sync_record(cur, ctx: Context, entity: str, record_id: str) -> str:
    # A line event re-syncs its whole document: PO lines need values from the
    # PO header, and a full re-read also removes lines deleted in BC.
    if entity == "purchaseOrderLines":
        line = _get(ctx, f"purchaseOrderLines({record_id})")
        if line is None:
            return "line no longer in BC; its PO's next event will tidy up"
        return sync_record(cur, ctx, "purchaseOrders", line["documentId"])

    if entity == "purchaseReceiptLines":
        line = _get(ctx, f"purchaseReceiptLines({record_id})")
        if line is None:
            return "line no longer in BC"
        return sync_record(cur, ctx, "purchaseReceipts", line["documentId"])

    rec = _get(ctx, f"{entity}({record_id})")
    if rec is None:
        return "record no longer in BC"
    return write_record(cur, ctx, entity, rec)


# ---------------------------------------------------------------------------
# Deletes
#   vendors            -> not deleted in BC (blocked instead, which the
#                         normal update maps to Mst_Vendors.isActive)
#   purchaseOrders     -> header and all its lines deleted
#   purchaseOrderLines -> that line deleted
#   anything else      -> logged, not applied (CF team to confirm)
# ---------------------------------------------------------------------------

def delete_purchase_order(cur, ctx: Context, bc_id: str) -> str:
    # Same lock as upsert_purchase_order, so a delete can't interleave with
    # an update of the same PO.
    lock_key(cur, "AP_DC_PurchaseOrder", ctx.company_id, bc_id)

    cur.execute(
        "SELECT DISTINCT POnumber FROM dbo.AP_DC_PurchaseOrder "
        "WHERE CompanyCode = %s AND POInternalID = %s",
        (ctx.company_id, bc_id),
    )
    numbers = [r[0] for r in cur.fetchall() if r[0]]

    lines = 0
    for number in numbers:
        cur.execute(
            "DELETE FROM dbo.AP_DC_PurchaseOrderLines "
            "WHERE CLIENT_ID = %s AND COMPANY_ID = %s AND PONumber = %s",
            (ctx.client_id, ctx.company_id, number),
        )
        lines += cur.rowcount or 0

    cur.execute(
        "DELETE FROM dbo.AP_DC_PurchaseOrder "
        "WHERE CompanyCode = %s AND POInternalID = %s",
        (ctx.company_id, bc_id),
    )
    headers = cur.rowcount or 0
    if not headers:
        return "PO not in AP_DC_PurchaseOrder; nothing to delete"
    return (f"PO {', '.join(map(str, numbers))} deleted "
            f"({headers} header, {lines} line(s))")


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


def _context(conn, bc_guid):
    company_id = cfg.cf_company_id(bc_guid)
    if company_id is None or company_id not in cfg.COMPANY_IDS:
        return None
    return Context(conn, bc_guid, company_id)


def handle_event(conn, change_type: str, entity: str, bc_guid: str,
                 record_id: str) -> str:
    ctx = _context(conn, bc_guid)
    if ctx is None:
        log.info("Skipping %s %s: BC company %s is not in CF_BC_COMPANY_MAP "
                 "or not in CF_COMPANY_IDS", entity, record_id, bc_guid)
        return "out of scope"

    if change_type == "deleted":
        handler = DELETE_HANDLERS.get(entity)
        if handler is None:
            log.warning("BC deleted %s %s (company %s) - not applied; delete "
                        "handling for %s is pending with the CF team",
                        entity, record_id, ctx.company_id, entity)
            return "delete not applied"
    else:
        handler = None

    cur = conn.cursor()
    try:
        if handler:
            result = handler(cur, ctx, record_id)
        else:
            result = sync_record(cur, ctx, entity, record_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    log.info("cf_direct %s %s (company %s): %s",
             entity, record_id, ctx.company_id, result)
    return result


# ---------------------------------------------------------------------------
# Backfill - re-read from BC and upsert again
# ---------------------------------------------------------------------------

def backfill(conn, entity: str, company_ids=None, modified_since=None,
             top=None) -> dict:
    """
    Re-pull an entity from BC and upsert every record. Use it to recover
    from missed or dead-lettered events, and to refresh rows after a lookup
    value (payment terms, item category) changes in BC.

    modified_since ('2026-10-01') limits it to records changed since then -
    use it for purchaseOrders / purchaseReceipts, which also fetch every
    document's lines.
    """
    if entity not in BACKFILL_ENTITIES:
        raise ValueError(f"entity must be one of {BACKFILL_ENTITIES}")

    targets = company_ids or cfg.COMPANY_IDS
    summary = {"entity": entity, "companies": {}}

    for company_id in targets:
        bc_guid = cfg.bc_company_guid(company_id)
        ctx = Context(conn, bc_guid, company_id)

        query = []
        if modified_since:
            query.append(f"$filter=lastModifiedDateTime gt "
                         f"{modified_since}T00:00:00Z")
        if top:
            query.append(f"$top={int(top)}")
        if entity == "dimensionValues":
            # Only the DIVISION dimension's values: find it, then read them.
            records = []
            for d in bc_get_all(f"{ctx.base}/dimensions",
                                timeout=cfg.BC_HTTP_TIMEOUT):
                ctx._cache[("dim", d["id"])] = (d.get("code"),
                                                d.get("displayName"))
                if ctx.is_division(d["id"]):
                    records += bc_get_all(
                        f"{ctx.base}/dimensions({d['id']})/dimensionValues",
                        timeout=cfg.BC_HTTP_TIMEOUT)
        else:
            url = (f"{ctx.base}/{entity}"
                   + ("?" + "&".join(query) if query else ""))
            records = bc_get_all(url, timeout=cfg.BC_HTTP_TIMEOUT)

        counts = {"read": len(records), "inserted": 0, "updated": 0,
                  "skipped": 0, "failed": 0}
        errors = []

        for rec in records:
            cur = conn.cursor()
            try:
                result = write_record(cur, ctx, entity, rec)
                conn.commit()
                if result.startswith("inserted"):
                    counts["inserted"] += 1
                elif result.startswith("updated"):
                    counts["updated"] += 1
                else:
                    counts["skipped"] += 1
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