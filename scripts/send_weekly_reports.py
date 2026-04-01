"""
AMZ Prep — Weekly Lead Report Email Sender
==========================================
Replaces Zapier entirely. Runs every Monday at 3PM UTC (10AM EST).

For each active partner:
  1. Fetches all contacts from the partner's HubSpot recipient list
  2. Enrolls each contact individually in the partner's HubSpot workflow
  3. HubSpot workflow: Set marketing contact → Send email → End

This means every person on the list gets their own individual email,
with no shared inbox privacy risk.

Required GitHub Secrets:
  HUBSPOT_TOKEN    — HubSpot Private App token

Optional:
  PARTNER_FILTER   — Comma-separated partner names (leave blank = all)
  DRY_RUN          — "true" to log only, no enrollments made

partners.json fields used by this script:
  partner_name        — for logging
  hubspot_list_id     — HubSpot Static List ID (add recipients here)
  hubspot_workflow_id — HubSpot Workflow ID to enroll contacts into
  active              — only processes active: true partners
"""

import os, sys, json, time, urllib.request, urllib.error
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────
HUBSPOT_TOKEN  = os.environ.get("HUBSPOT_TOKEN", "")
PARTNER_FILTER = os.environ.get("PARTNER_FILTER", "").strip()
DRY_RUN        = os.environ.get("DRY_RUN", "false").lower() == "true"
HS_BASE        = "https://api.hubapi.com"

# ── Logging ─────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    icons = {"INFO":"[INFO]","WARN":"[WARN]","ERROR":"[ERROR]","DEBUG":"[DEBUG]"}
    print(f"{icons.get(level,'     ')} [{ts}] {msg}", flush=True)

# ── HubSpot API ─────────────────────────────────────────────────────
def hs(method, path, body=None):
    url     = f"{HS_BASE}{path}"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    data    = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            resp_body = r.read().decode("utf-8")
            return r.status, json.loads(resp_body) if resp_body else {}
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8")
        log(f"HS {e.code} {method} {path}: {err[:200]}", "ERROR")
        return e.code, {}

def get_list_contacts(list_id):
    """Fetch all contacts from a HubSpot Static List."""
    contacts = []
    after    = None

    while True:
        path = f"/crm/v3/lists/{list_id}/memberships?limit=100"
        if after:
            path += f"&after={after}"

        status, resp = hs("GET", path)

        if status != 200:
            log(f"Failed to fetch list {list_id}: HTTP {status}", "ERROR")
            break

        results = resp.get("results", [])
        for r in results:
            cid = r.get("recordId")
            if cid:
                contacts.append(str(cid))

        paging = resp.get("paging", {}).get("next", {})
        after  = paging.get("after") if paging else None
        if not after:
            break
        time.sleep(0.1)

    return contacts

def get_contact_email(contact_id):
    """Get email address for a contact by ID."""
    status, resp = hs("GET", f"/crm/v3/objects/contacts/{contact_id}?properties=email,hs_marketable_status")
    if status == 200:
        props = resp.get("properties", {})
        return props.get("email", ""), props.get("hs_marketable_status", "")
    return "", ""

def set_marketing_contact(contact_id):
    """Ensure contact is set as marketing contact before enrollment."""
    status, _ = hs("POST", "/contacts/v1/contacts/vid/{}/profile".format(contact_id), {
        "properties": [{"property": "hs_marketable_status", "value": "true"}]
    })
    return status in (200, 204)

def enroll_in_workflow(workflow_id, email):
    """Enroll a contact in a HubSpot workflow by email."""
    if DRY_RUN:
        log(f"  DRY RUN — would enroll {email} in workflow {workflow_id}", "DEBUG")
        return True
    status, _ = hs(
        "POST",
        f"/automation/v2/workflows/{workflow_id}/enrollments/contacts/{urllib.request.quote(email)}"
    )
    return status in (200, 204)

# ── Main ─────────────────────────────────────────────────────────────
def main():
    log("=" * 55)
    log("AMZ Prep — Weekly Lead Report Email Sender")
    log(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN: log("MODE: DRY RUN — no enrollments will be made", "WARN")
    log("=" * 55)

    if not HUBSPOT_TOKEN:
        log("HUBSPOT_TOKEN not set", "ERROR"); sys.exit(1)

    config_path = os.path.join(os.path.dirname(__file__), "..", "partners.json")
    with open(config_path) as f:
        config = json.load(f)

    partners = [p for p in config["partners"] if p.get("active", True)]
    if PARTNER_FILTER:
        names    = [x.strip() for x in PARTNER_FILTER.split(",")]
        partners = [p for p in partners if p["partner_name"] in names]

    # Only process partners that have both list_id and workflow_id configured
    partners = [
        p for p in partners
        if p.get("hubspot_list_id") and p.get("hubspot_workflow_id")
        and str(p["hubspot_list_id"]) != "REPLACE"
        and str(p["hubspot_workflow_id"]) != "REPLACE"
    ]

    if not partners:
        log("No partners with hubspot_list_id + hubspot_workflow_id configured.", "WARN")
        log("Add these fields to partners.json to enable list-based sending.")
        sys.exit(0)

    log(f"Partners to send: {[p['partner_name'] for p in partners]}")

    results = []
    for partner in partners:
        name        = partner["partner_name"]
        list_id     = str(partner["hubspot_list_id"])
        workflow_id = str(partner["hubspot_workflow_id"])

        log(f"\n--- {name} ---")
        log(f"  List ID: {list_id}  |  Workflow ID: {workflow_id}")

        # Step 1 — Get all contacts in the recipient list
        contact_ids = get_list_contacts(list_id)
        log(f"  {len(contact_ids)} recipient(s) found in list")

        if not contact_ids:
            log(f"  List is empty — skipping", "WARN")
            results.append({"partner": name, "sent": 0, "failed": 0, "status": "empty_list"})
            continue

        # Step 2 — Enroll each contact in the workflow
        sent = 0; failed = 0
        for cid in contact_ids:
            email, mkt_status = get_contact_email(cid)
            if not email:
                log(f"  Contact {cid}: no email — skipping", "WARN")
                failed += 1
                continue

            # Ensure marketing contact status is true
            if mkt_status != "true":
                set_marketing_contact(cid)
                time.sleep(0.1)

            ok = enroll_in_workflow(workflow_id, email)
            status_str = "enrolled" if ok else "FAILED"
            log(f"  {email}: {status_str}")
            if ok: sent += 1
            else:  failed += 1
            time.sleep(0.3)   # rate limit buffer

        log(f"  Result: {sent} sent, {failed} failed")
        results.append({"partner": name, "sent": sent, "failed": failed,
                         "status": "ok" if failed == 0 else "partial"})
        time.sleep(0.5)

    # Summary
    log("\n" + "=" * 55)
    log("SEND SUMMARY")
    col_p = 24; col_s = 7; col_f = 7; col_st = 12
    sep = "-" * (col_p + col_s + col_f + col_st + 6)
    log(f"{'Partner':<{col_p}}  {'Sent':>{col_s}}  {'Failed':>{col_f}}  {'Status':<{col_st}}")
    log(sep)
    any_fail = False
    for r in results:
        log(f"{r['partner']:<{col_p}}  {r['sent']:>{col_s}}  {r['failed']:>{col_f}}  {r['status']:<{col_st}}")
        if r["failed"] > 0: any_fail = True
    log(sep)

    if any_fail:
        log("Some enrollments failed — check workflow IDs and contact marketing status.", "WARN")
        sys.exit(1)
    log("All enrollments complete.")

if __name__ == "__main__":
    main()
