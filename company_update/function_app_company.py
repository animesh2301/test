import json
import logging
import os
import re
import time
from typing import List

import azure.functions as func
import pymssql
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

import cf_direct
import health_check
import invoice_poster
import uat_config as uat
from bc_http import bc_get, bc_get_all, bc_request

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# Shared config / helpers
# ---------------------------------------------------------------------------

EXPECTED_CLIENT_STATE = os.environ["BC_CLIENT_STATE"]
EVENTHUB_CONN_STR = os.environ["EventHubConnection"]
EVENTHUB_NAME = "bc-ds-events"

SQL_SERVER = os.environ["SQL_SERVER"]      # "<server>.database.windows.net"
SQL_DATABASE = os.environ["SQL_DATABASE"]
SQL_USER = os.environ["SQL_USER"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]

BC_API_BASE = (
    "https://api.businesscentral.dynamics.com/v2.0/"
    f"{os.environ.get('BC_ENVIRONMENT', 'Sandbox-June-2026')}"
)
BC_HTTP_TIMEOUT = 30

COMPANY_CONFIG: dict[str, str] = json.loads(os.environ.get("BC_COMPANY_ID", "{}"))

# ---------------------------------------------------------------------------
# Entities that land in bc.stg_* tables. Everything in
# cf_direct.DIRECT_ENTITIES (vendors, POs, receipts) is handled by cf_direct
# instead.
#
# Key = the entity name as it appears in the webhook "resource" path.
# Columns = the BC field names. Every row also gets company_name (from
# BC_COMPANY_ID) and company_id (the BC company id), as in the table DDL.
# ---------------------------------------------------------------------------

ENTITY_CONFIG = {
    "accounts": {
        "table": "bc.stg_accounts",
        "columns": ["accountType", "blocked", "category",
                    "consolidationCreditAccount", "consolidationDebitAccount",
                    "consolidationTranslationMethod", "directPosting",
                    "displayName", "excludeFromConsolidation", "id",
                    "lastModifiedDateTime", "netChange", "number",
                    "subCategory"],
    },
    "dimensionValues": {
        "table": "bc.stg_dimensionValues",
        "columns": ["id", "code", "dimensionId", "displayName",
                    "consolidationCode", "lastModifiedDateTime"],
    },
    "locations": {
        "table": "bc.stg_locations",
        "columns": ["addressLine1", "addressLine2", "city", "code", "contact",
                    "country", "displayName", "email", "id",
                    "lastModifiedDateTime", "phoneNumber", "postalCode",
                    "state", "website"],
    },
    "paymentTerms": {
        "table": "bc.stg_paymentTerms",
        "columns": ["calculateDiscountOnCreditMemos", "code",
                    "discountDateCalculation", "discountPercent",
                    "displayName", "dueDateCalculation", "id",
                    "lastModifiedDateTime"],
    },
    "paymentMethods": {
        "table": "bc.stg_paymentMethods",
        "columns": ["id", "code", "displayName", "lastModifiedDateTime"],
    },
    "items": {
        "table": "bc.stg_items",
        "columns": ["id", "number", "displayName", "type", "itemCategoryId",
                    "itemCategoryCode", "blocked", "baseUnitOfMeasureId",
                    "baseUnitOfMeasureCode", "gtin", "inventory",
                    "unitPrice", "priceIncludesTax", "unitCost",
                    "taxGroupId", "taxGroupCode",
                    "generalProductPostingGroupId",
                    "generalProductPostingGroupCode",
                    "inventoryPostingGroupId", "inventoryPostingGroupCode",
                    "lastModifiedDateTime"],
    },
    "purchaseInvoices": {
        "table": "bc.stg_purchaseInvoices",
        "columns": ["buyFromAddressLine1", "buyFromAddressLine2", "buyFromCity",
                    "buyFromCountry", "buyFromPostCode", "buyFromState",
                    "currencyCode", "currencyId", "discountAmount",
                    "discountAppliedBeforeTax", "dueDate", "id", "invoiceDate",
                    "lastModifiedDateTime", "number", "orderId", "orderNumber",
                    "payToAddressLine1", "payToAddressLine2", "payToCity",
                    "payToContact", "payToCountry", "payToName",
                    "payToPostCode", "payToState", "payToVendorId",
                    "payToVendorNumber", "postingDate", "pricesIncludeTax",
                    "purchaser", "shipToAddressLine1", "shipToAddressLine2",
                    "shipToCity", "shipToContact", "shipToCountry",
                    "shipToName", "shipToPostCode", "shipToState",
                    "shortcutDimension1Code", "shortcutDimension2Code",
                    "status", "totalAmountExcludingTax",
                    "totalAmountIncludingTax", "totalTaxAmount", "vendorId",
                    "vendorInvoiceNumber", "vendorName", "vendorNumber"],
    },
    "purchaseInvoiceLines": {
        "table": "bc.stg_purchaseInvoiceLines",
        "columns": ["id", "documentId", "sequence", "itemId", "accountId",
                    "lineType", "lineObjectNumber", "description",
                    "description2", "unitOfMeasureId", "unitOfMeasureCode",
                    "quantity", "unitCost", "discountAmount",
                    "discountPercent", "discountAppliedBeforeTax",
                    "amountExcludingTax", "taxCode", "taxPercent",
                    "totalTaxAmount", "amountIncludingTax",
                    "invoiceDiscountAllocation", "netAmount", "netTaxAmount",
                    "netAmountIncludingTax", "expectedReceiptDate",
                    "itemVariantId", "locationId"],
    },
}

# Lines have no webhook of their own; the parent document's event re-reads
# them. parent entity -> line entity (linked on documentId).
CHILD_ENTITY = {"purchaseInvoices": "purchaseInvoiceLines"}


def extract_company_entity_and_id(resource: str) -> tuple[str, str, str] | tuple[None, None, None]:
    # resource looks like: api/v2.0/companies(...)/vendors(26814998-936a-401c-81c1-0e848a64971d)
    m = re.search(r"companies\(([0-9a-fA-F-]+)\)/([A-Za-z]+)\(([0-9a-fA-F-]+)\)\s*$", resource)
    if not m:
        return None, None, None
    return m.group(1), m.group(2), m.group(3)


def get_sql_connection():
    return pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
    )


class SqlSession:
    """One SQL connection, opened on first use and reused across a batch."""

    def __init__(self):
        self._conn = None

    @property
    def conn(self):
        if self._conn is None:
            self._conn = get_sql_connection()
        return self._conn

    def reset(self):
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            logging.debug("Ignoring error closing a SQL connection",
                          exc_info=True)
        finally:
            self._conn = None

    close = reset


# ---------------------------------------------------------------------------
# Function 1 — BC Webhook Receiver (HTTP trigger)
#   Answers the validation handshake and forwards notifications to Event Hub.
# ---------------------------------------------------------------------------

@app.route(route="BcWebhookReceiver", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
async def BcWebhookReceiver(req: func.HttpRequest) -> func.HttpResponse:
    validation_token = req.params.get("validationToken")
    if validation_token:
        return func.HttpResponse(validation_token, status_code=200, mimetype="text/plain")

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Bad request", status_code=400)

    items = body.get("value", [])
    to_send = [i for i in items if i.get("clientState") == EXPECTED_CLIENT_STATE]

    dropped = len(items) - len(to_send)
    if dropped:
        logging.warning("Dropped %d event(s) with mismatched clientState", dropped)

    if to_send:
        producer = EventHubProducerClient.from_connection_string(
            EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME
        )
        async with producer:
            for item in to_send:
                _, _, record_id = extract_company_entity_and_id(item.get("resource", ""))
                # Same record -> same partition, so its events stay in order.
                batch = await producer.create_batch(partition_key=record_id)
                batch.add(EventData(json.dumps(item)))
                await producer.send_batch(batch)

    return func.HttpResponse(status_code=200)


# ---------------------------------------------------------------------------
# Function 2 — Event Processor (Event Hub trigger)
#   cf_direct entities go straight to the CF dbo tables; the rest land in
#   bc.stg_*.
# ---------------------------------------------------------------------------

def upsert_stg(conn, entity: str, record: dict, company_id: str,
               company_name: str) -> None:
    """MERGE one BC record into its bc.stg_ table (keyed on id + company_id)."""
    table = ENTITY_CONFIG[entity]["table"]
    columns = ["company_name", "company_id"] + ENTITY_CONFIG[entity]["columns"]
    params = {c: cf_direct.val(record.get(c)) for c in columns}
    params.update(company_name=company_name, company_id=company_id)

    conn.cursor().execute(
        f"""
        MERGE {table} AS target
        USING (SELECT %(id)s AS id, %(company_id)s AS company_id) AS src
        ON target.id = src.id AND target.company_id = src.company_id
        WHEN MATCHED THEN UPDATE SET
            {", ".join(f"{c} = %({c})s" for c in columns
                       if c not in ("id", "company_id"))}
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(columns)})
            VALUES ({", ".join(f"%({c})s" for c in columns)});
        """,
        params,
    )
    conn.commit()


def apply_stg_event(conn, change_type, entity, record_id, resource,
                    company_id, company_name) -> None:
    table = ENTITY_CONFIG[entity]["table"]
    child = CHILD_ENTITY.get(entity)
    cursor = conn.cursor()

    if change_type == "deleted":
        # Hard delete; a deleted document's lines go with it.
        cursor.execute(f"DELETE FROM {table} WHERE id = %s AND company_id = %s",
                       (record_id, company_id))
        conn.commit()
        if child:
            cursor.execute(
                f"DELETE FROM {ENTITY_CONFIG[child]['table']} "
                f"WHERE documentId = %s AND company_id = %s",
                (record_id, company_id))
            conn.commit()
        return

    record = bc_get(f"{BC_API_BASE}/{resource.lstrip('/')}",
                    timeout=BC_HTTP_TIMEOUT)
    upsert_stg(conn, entity, record, company_id, company_name)

    if child:
        # Replace the document's lines with what BC holds now, in one
        # transaction: delete-then-insert also drops lines removed in BC.
        # A failure leaves the old lines (BcEventProcessor resets the
        # connection, which rolls back).
        lines = bc_get_all(f"{BC_API_BASE}/{resource.lstrip('/')}/{child}",
                           allow_404=True, timeout=BC_HTTP_TIMEOUT)
        line_table = ENTITY_CONFIG[child]["table"]
        columns = ["company_name", "company_id"] + ENTITY_CONFIG[child]["columns"]

        cursor.execute(
            f"DELETE FROM {line_table} WHERE documentId = %s AND company_id = %s",
            (record_id, company_id))
        removed = cursor.rowcount
        if lines:
            cursor.executemany(
                f"INSERT INTO {line_table} ({', '.join(columns)}) "
                f"VALUES ({', '.join('%s' for _ in columns)})",
                [(company_name, company_id)
                 + tuple(cf_direct.val(line.get(c))
                         for c in ENTITY_CONFIG[child]["columns"])
                 for line in lines])
        conn.commit()
        logging.info("Synced %s for %s %s (%s): %s row(s) replaced %s",
                     child, entity, record_id, company_name,
                     len(lines), removed)


def dead_letter(raw_payload: str, error: Exception) -> None:
    conn = get_sql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO bc.FailedEvents (Payload, [Error], FailedOn) VALUES (%s, %s, SYSUTCDATETIME())",
            (raw_payload, str(error)),
        )
        conn.commit()
    finally:
        conn.close()


def process_event(raw_body: str, sql: SqlSession) -> None:
    item = json.loads(raw_body)
    change_type = item.get("changeType")
    resource = item.get("resource", "")

    if change_type == "collection":
        # >1000 records changed within 30s: BC sends a filter, not keys.
        # Run CfBackfill for the entity to catch up.
        logging.warning("Bulk change received - run a backfill. filter=%s",
                        resource)
        return

    company_id, entity, record_id = extract_company_entity_and_id(resource)
    if entity is None:
        logging.warning("Could not parse entity/id from resource: %s", resource)
        return

    if entity not in ENTITY_CONFIG and entity not in cf_direct.DIRECT_ENTITIES:
        # Surfaces loudly (dead letter) instead of silently dropping it.
        raise ValueError(f"No ENTITY_CONFIG entry for entity '{entity}' (resource={resource})")

    company_name = COMPANY_CONFIG.get(company_id)
    if company_name is None:
        raise ValueError(f"No BC_COMPANIES entry for company id '{company_id}' (resource={resource})")

    if entity in cf_direct.DIRECT_ENTITIES:
        cf_direct.handle_event(sql.conn, change_type, entity, company_id,
                               record_id)
    else:
        apply_stg_event(sql.conn, change_type, entity, record_id, resource,
                        company_id, company_name)


@app.function_name(name="BcEventProcessor")
@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="bc-ds-events",
    connection="EventHubConnection",
    cardinality="many",
)
def BcEventProcessor(events: List[func.EventHubEvent]) -> None:
    if isinstance(events, func.EventHubEvent):
        events = [events]

    sql = SqlSession()
    failed = 0

    try:
        for event in events:
            raw = event.get_body().decode("utf-8")
            try:
                process_event(raw, sql)
            except Exception as ex:
                failed += 1
                logging.exception("Failed to process event")
                # dead_letter opens its own connection on purpose: the batch
                # connection may be the thing that just broke.
                dead_letter(raw, ex)
                sql.reset()
    finally:
        sql.close()

    if failed:
        logging.warning("Batch of %d event(s) finished with %d failure(s)",
                        len(events), failed)


# ---------------------------------------------------------------------------
# Renewal — Timer trigger
#   BC webhook subscriptions expire after ~3 days; this renews them every 12h.
#   One failing renewal doesn't block the others.
#
#   Which subscriptions are renewed:
#
#   1. BC_SUBSCRIPTION_IDS unset or empty  ->  AUTO (recommended).
#      Lists /subscriptions and renews every one whose notificationUrl points
#      at this Function App. Both companies' subscriptions are included -
#      subscriptions live at environment level, not under a company - and a
#      subscription you re-create picks up a new id with no setting to edit.
#
#   2. BC_SUBSCRIPTION_IDS set  ->  renews exactly those ids. Each value may be
#      one id or a list, so one entity can have a subscription per company:
#        {"vendors": ["<id-889>", "<id-890>"],
#         "purchaseOrders": ["<id-889>", "<id-890>"],
#         "paymentTerms": "<single-id>"}
#
#   NOTE: no subscription is needed for purchaseOrderLines or
#   purchaseInvoiceLines — they ride on their parent document's subscription.
# ---------------------------------------------------------------------------


def _subscriptions_base() -> str:
    return (
        f"https://api.businesscentral.dynamics.com/v2.0/{os.environ['BC_TENANT_ID']}/"
        f"{os.environ['BC_ENVIRONMENT']}/api/v2.0/subscriptions"
    )


def _configured_subscription_ids() -> list[tuple[str, str]]:
    """BC_SUBSCRIPTION_IDS -> [(label, subscription_id)]. Accepts str or list values."""
    raw = json.loads(os.environ.get("BC_SUBSCRIPTION_IDS", "") or "{}")
    pairs: list[tuple[str, str]] = []
    for entity, value in raw.items():
        ids = value if isinstance(value, list) else [value]
        for sub_id in ids:
            if sub_id:
                pairs.append((entity, str(sub_id)))
    return pairs


def _our_subscriptions(base: str) -> list[dict]:
    """Every subscription whose notificationUrl points at this Function App."""
    rows = bc_get_all(base)
    host = (os.environ.get("BC_WEBHOOK_HOST")
            or os.environ.get("WEBSITE_HOSTNAME", "")).lower()
    if not host:
        logging.warning("WEBSITE_HOSTNAME not set - renewing every subscription "
                        "this app registration can see (%d).", len(rows))
        return rows
    return [r for r in rows if host in (r.get("notificationUrl") or "").lower()]


def _renew(base: str, sub: dict) -> dict | None:
    """
    PATCH one subscription. BC requires an If-Match header; "*" matches any
    version. BC re-validates notificationUrl (the receiver answers the
    validationToken handshake, as it did when the subscription was created).
    """
    body = {k: sub[k] for k in ("notificationUrl", "resource", "clientState")
            if sub.get(k)}
    return bc_request(
        "PATCH",
        f"{base}('{sub['subscriptionId']}')",
        json_body=body,
        etag="*",          # any version - no 412 if BC changed it since the list
        timeout=30,
    )


def _company_of(resource: str) -> str:
    m = re.search(r"companies\(([^)]+)\)", resource or "")
    return m.group(1) if m else "?"


@app.function_name(name="RenewBcSubscription")
@app.timer_trigger(schedule="0 0 */12 * * *", arg_name="timer", run_on_startup=False)
def RenewBcSubscription(timer: func.TimerRequest) -> None:
    base = _subscriptions_base()
    configured = _configured_subscription_ids()

    missing = 0
    if configured:
        targets = []
        for label, sub_id in configured:
            try:
                sub = bc_get(f"{base}('{sub_id}')", allow_404=True)
            except Exception:
                logging.exception("Failed to read %s subscription (%s)", label, sub_id)
                missing += 1
                continue
            if sub is None:
                logging.error(
                    "%s subscription %s no longer exists in BC (expired or deleted). "
                    "Re-create it and update BC_SUBSCRIPTION_IDS.", label, sub_id)
                missing += 1
                continue
            targets.append((label, sub))
        mode = "BC_SUBSCRIPTION_IDS"
    else:
        try:
            subs = _our_subscriptions(base)
        except Exception:
            logging.exception("Failed to list BC subscriptions")
            return
        targets = [((s.get("resource") or "").rsplit("/", 1)[-1], s) for s in subs]
        mode = "auto"

    if not targets and not missing:
        logging.warning("RenewBcSubscription (%s): nothing to renew.", mode)
        return

    renewed, failed = 0, missing
    for label, sub in targets:
        sub_id = sub.get("subscriptionId")
        company = _company_of(sub.get("resource"))
        try:
            try:
                resp = _renew(base, sub) or {}
            except Exception as first:
                # e.g. BC's check of the webhook URL timed out: wait, retry once
                logging.warning("Renew %s subscription %s (company %s) failed, "
                                "retrying once: %s %s", label, sub_id, company,
                                first, getattr(first, "body", "") or "")
                time.sleep(5)
                resp = _renew(base, sub) or {}
            renewed += 1
            logging.info("Renewed %s subscription %s (company %s), new expiration: %s",
                         label, sub_id, company, resp.get("expirationDateTime"))
        except Exception as e:
            failed += 1
            logging.exception("Failed to renew %s subscription %s (company %s): %s",
                              label, sub_id, company, getattr(e, "body", "") or e)

    logging.info("RenewBcSubscription (%s): %d renewed, %d failed", mode, renewed, failed)
    if failed:
        # Surfaces as a failed run in the portal / App Insights
        raise RuntimeError(f"{failed} subscription renewal(s) failed")


# ---------------------------------------------------------------------------
# Invoice posting, PO and non-PO (ContinuFlow tables -> BC)
#
#   AP_DataConversions  STATUS='APPROVED', ERP_POST='N', client/company in
#                       scope
#     -> AP_DC_InvoiceHeader + AP_DC_GL_Details
#       -> BC purchaseInvoices + lines + dimensionSetLines
#     -> ERP_POST='P' (or 'E' once retries are used up)
#
# AP_POST_MODE 'draft': the invoice is created and ERP_POST is set, but a BC
# user reviews and posts it; 'post' posts it straight away.
# ---------------------------------------------------------------------------

@app.function_name(name="PostInvoices")
@app.timer_trigger(schedule=uat.POST_CRON, arg_name="timer",
                   run_on_startup=False)
def PostInvoices(timer: func.TimerRequest) -> None:
    sql = SqlSession()
    try:
        summary = invoice_poster.run_batch(sql.conn)
        if summary["picked"]:
            logging.info("Purchase Invoice run: %s", summary)
    except Exception:
        logging.exception("PostInvoices failed")
        raise
    finally:
        sql.close()


@app.route(route="ap/post-invoices", auth_level=func.AuthLevel.FUNCTION,
           methods=["POST"])
def PostInvoicesNow(req: func.HttpRequest) -> func.HttpResponse:
    """
    Manual trigger, for UAT.

      POST /api/ap/post-invoices                  every pending record
      POST /api/ap/post-invoices?recordId=12345   one record, ignoring
                                                  STATUS and ERP_POST
      POST /api/ap/post-invoices?limit=1          just the first pending one
    """
    record_id = req.params.get("recordId")
    limit = req.params.get("limit")

    sql = SqlSession()
    try:
        summary = invoice_poster.run_batch(
            sql.conn,
            limit=int(limit) if limit else None,
            record_id=int(record_id) if record_id else None,
        )
        return func.HttpResponse(json.dumps(summary, indent=2, default=str),
                                 status_code=200,
                                 mimetype="application/json")
    except Exception as e:
        logging.exception("PostNonPoInvoicesNow failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, default=str),
            status_code=500, mimetype="application/json")
    finally:
        sql.close()


@app.route(route="ap/pending", auth_level=func.AuthLevel.FUNCTION,
           methods=["GET"])
def PendingInvoices(req: func.HttpRequest) -> func.HttpResponse:
    """Read-only: what the timer would pick up on its next run."""
    sql = SqlSession()
    try:
        rows = invoice_poster.pending_records(sql.conn)
        return func.HttpResponse(
            json.dumps({
                "clientId": uat.CLIENT_ID,
                "companyIds": uat.COMPANY_IDS,
                "status": uat.DC_STATUS,
                "erpPost": uat.DC_ERP_POST_PENDING,
                "postMode": uat.POST_MODE,
                "pending": rows,
            }, indent=2, default=str),
            status_code=200, mimetype="application/json")
    finally:
        sql.close()


# ---------------------------------------------------------------------------
# Backfill - re-read from BC and upsert into the ContinuFlow tables
#
# Direct loading means a dbo row is only as current as the last webhook that
# reached it. Use this after missed or dead-lettered events, or when a lookup
# value changed in BC (payment terms, item category) without the vendor / PO
# itself changing.
#
#   POST /api/cf/backfill?entity=vendors
#   POST /api/cf/backfill?entity=purchaseOrders&modifiedSince=2026-10-01
#   POST /api/cf/backfill?entity=vendors&company=889
#
# entity: vendors | purchaseOrders | purchaseReceipts
# The HTTP gateway cuts responses off after ~230s; use modifiedSince for the
# document entities, which also fetch every document's lines.
# ---------------------------------------------------------------------------

@app.route(route="cf/backfill", auth_level=func.AuthLevel.FUNCTION,
           methods=["POST"])
def CfBackfill(req: func.HttpRequest) -> func.HttpResponse:
    entity = req.params.get("entity")
    company = req.params.get("company")
    modified_since = req.params.get("modifiedSince")
    top = req.params.get("top")

    if entity not in cf_direct.BACKFILL_ENTITIES:
        return func.HttpResponse(
            json.dumps({"error": f"entity must be one of "
                                 f"{list(cf_direct.BACKFILL_ENTITIES)}"}),
            status_code=400, mimetype="application/json")

    sql = SqlSession()
    try:
        summary = cf_direct.backfill(
            sql.conn, entity,
            company_ids=[int(company)] if company else None,
            modified_since=modified_since,
            top=int(top) if top else None,
        )
        return func.HttpResponse(json.dumps(summary, indent=2, default=str),
                                 status_code=200,
                                 mimetype="application/json")
    except Exception as e:
        logging.exception("CfBackfill failed")
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}, default=str),
            status_code=500, mimetype="application/json")
    finally:
        sql.close()


# ---------------------------------------------------------------------------
# Health check - every 15 min, read-only. Logs "HEALTHCHECK A3/A4/A5 ..."
# warnings that the Azure Monitor alerts fire on.
# ---------------------------------------------------------------------------

@app.function_name(name="IntegrationHealthCheck")
@app.timer_trigger(schedule="0 */15 * * * *", arg_name="timer", run_on_startup=False)
def IntegrationHealthCheck(timer: func.TimerRequest) -> None:
    sql = SqlSession()
    try:
        health_check.run(sql.conn)
    finally:
        sql.close()
