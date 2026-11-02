"""
Integration health check - the SQL-side conditions Application Insights
can't see on its own.

Runs every 15 minutes (IntegrationHealthCheck in function_app.py). Every
problem found is logged as ONE warning starting with "HEALTHCHECK A<n>";
the Azure Monitor alerts A3, A4 and A5 fire on those lines. Read-only:
nothing is written to any table.
"""

import logging

import uat_config as cfg

log = logging.getLogger("health_check")

WINDOW_MIN = 15      # same as the timer interval
STUCK_MIN = 60       # an invoice still pending this long after its first try


def _ids(rows):
    ids = [str(r[0]) for r in rows]
    return ", ".join(ids[:20]) + (" ..." if len(ids) > 20 else "")


def run(conn) -> None:
    cur = conn.cursor()

    # A3 - BC events that failed to load since the last check
    cur.execute(
        "SELECT COUNT(*) FROM bc.FailedEvents "
        "WHERE FailedOn > DATEADD(minute, -%s, SYSUTCDATETIME())",
        (WINDOW_MIN,),
    )
    failed_events = cur.fetchone()[0]
    if failed_events:
        log.warning("HEALTHCHECK A3 FailedEvents: %s new in the last %s min",
                    failed_events, WINDOW_MIN)

    # A4 - invoices that failed or were held since the last check
    cur.execute(
        "SELECT RecordId FROM bc.InvoicePostLog "
        "WHERE Step IN ('held', 'bc_error', 'error') "
        "  AND LoggedAt > DATEADD(minute, -%s, SYSUTCDATETIME()) "
        "GROUP BY RecordId",
        (WINDOW_MIN,),
    )
    failed = cur.fetchall()
    if failed:
        log.warning("HEALTHCHECK A4 invoice posting failed: %s record(s) "
                    "in the last %s min: %s", len(failed), WINDOW_MIN,
                    _ids(failed))

    # A5 - approved invoices still pending an hour after the first attempt
    cur.execute(
        f"""
        SELECT dc.RECORD_ID
        FROM dbo.AP_DataConversions dc
        WHERE {cfg.company_scope_sql('dc')}
          AND dc.STATUS = %s
          AND dc.ERP_POST = %s
          AND (SELECT MIN(l.LoggedAt) FROM bc.InvoicePostLog l
               WHERE l.RecordId = dc.RECORD_ID)
              < DATEADD(minute, -%s, SYSUTCDATETIME())
        """,
        (cfg.DC_STATUS, cfg.DC_ERP_POST_PENDING, STUCK_MIN),
    )
    stuck = cur.fetchall()
    if stuck:
        log.warning("HEALTHCHECK A5 stuck invoices: %s record(s) pending "
                    "over %s min: %s", len(stuck), STUCK_MIN, _ids(stuck))

    log.info("HEALTHCHECK done: failedEvents=%s failedInvoices=%s stuck=%s",
             failed_events, len(failed), len(stuck))
