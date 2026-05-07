"""
AMZ Prep — Weekly Lead Report Email Sender
==========================================
Replaces Zapier. Runs every Monday at 3PM UTC (10AM EST).

For each active partner:
  1. Fetches all contacts from the partner's HubSpot recipient list
  2. Removes them from the list, then immediately re-adds them
  3. HubSpot detects the re-membership → triggers the workflow automatically
  4. Workflow: Set marketing contact → Send email → End

This approach bypasses the legacy /automation/v2 enrollment API entirely,
using list operations which work with all workflow types including
workflows built in HubSpot's new editor.

Required GitHub Secrets:
  HUBSPOT_TOKEN    — HubSpot Private App token (needs crm.lists.read + crm.lists.write)

Required partners.json fields:
  hubspot_list_id     — HubSpot Static List ID
  hubspot_workflow_id — for reference/logging only (not used in API call)
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
            return r.status, json.loads(resp_body) if resp_body.strip() else {}
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8")
        log(f"HS {e.code} {method} {path}: {err[:300]}", "ERROR")
        return e.code, {}

def get_list_members(list_id):
    """Fetch all contact record IDs from a HubSpot Static List."""
    record_ids = []
    after      = None
    while True:
        path = f"/crm/v3/lists/{list_id}/memberships?limit=100"
        if after:
            path += f"&after={after}"
        status, resp = hs("GET", path)
        if status != 200:
            log(f"Failed to fetch list {list_id}: HTTP {status}", "ERROR")
            return []
        for r in resp.get("results", []):
            rid = r.get("recordId")
            if rid:
                record_ids.append(int(rid))
        nxt = resp.get("paging", {}).get("next", {})
        after = nxt.get("after") if nxt else None
        if not after:
            break
        time.sleep(0.1)
    return record_ids

def get_contact_emails(record_ids):
    """Batch fetch email addresses for a list of contact IDs."""
    emails = {}
    # Process in batches of 100
    for i in range(0, len(record_ids), 100):
        batch = record_ids[i:i+100]
        body  = {
            "inputs": [{"id": str(rid)} for rid in batch],
            "properties": ["email", "firstname", "lastname"]
        }
        status, resp = hs("POST", "/crm/v3/objects/contacts/batch/read", body)
        if status in (200, 207):
            for r in resp.get("results", []):
                rid   = int(r.get("id", 0))
                email = r.get("properties", {}).get("email", "")
                fn    = r.get("properties", {}).get("firstname", "") or ""
                ln    = r.get("properties", {}).get("lastname", "")  or ""
                name  = (fn + " " + ln).strip() or email
                if email:
                    emails[rid] = {"email": email, "name": name}
        time.sleep(0.2)
    return emails

def remove_from_list(list_id, record_ids):
    """Remove contacts from a HubSpot list."""
    if DRY_RUN:
        log(f"  DRY RUN — would remove {len(record_ids)} contact(s) from list {list_id}", "DEBUG")
        return True
    status, resp = hs("PUT",
        f"/crm/v3/lists/{list_id}/memberships/add-and-remove",
        {"recordIdsToAdd": [], "recordIdsToRemove": record_ids}
    )
    if status == 200:
        removed = resp.get("recordsIdsRemoved", [])
        log(f"  Removed {len(removed)} contact(s) from list")
        return True
    log(f"  Remove from list failed: HTTP {status}", "WARN")
    return False

def add_to_list(list_id, record_ids):
    """Add contacts back to a HubSpot list."""
    if DRY_RUN:
        log(f"  DRY RUN — would add {len(record_ids)} contact(s) to list {list_id}", "DEBUG")
        return True
    status, resp = hs("PUT",
        f"/crm/v3/lists/{list_id}/memberships/add-and-remove",
        {"recordIdsToAdd": record_ids, "recordIdsToRemove": []}
    )
    if status == 200:
        added = resp.get("recordIdsAdded", [])
        log(f"  Re-added {len(added)} contact(s) to list — workflow will trigger for each")
        return True
    log(f"  Add to list failed: HTTP {status}", "WARN")
    return False

# ── Main ─────────────────────────────────────────────────────────────
def main():
    log("=" * 55)
    log("AMZ Prep — Weekly Lead Report Email Sender")
    log(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN: log("MODE: DRY RUN — no list changes will be made", "WARN")
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

    # Only process partners with list_id configured
    partners = [
        p for p in partners
        if p.get("hubspot_list_id")
        and str(p["hubspot_list_id"]) != "REPLACE"
        and "REPLACE" not in str(p["hubspot_list_id"])
    ]

    if not partners:
        log("No partners with hubspot_list_id configured.", "WARN")
        sys.exit(0)

    log(f"Partners to process: {[p['partner_name'] for p in partners]}")

    results = []
    for partner in partners:
        name    = partner["partner_name"]
        list_id = str(partner["hubspot_list_id"])
        log(f"\n--- {name} (List: {list_id}) ---")

        # Step 1 — Get current list members
        record_ids = get_list_members(list_id)
        log(f"  {len(record_ids)} recipient(s) in list")

        if not record_ids:
            log("  List is empty — skipping", "WARN")
            results.append({"partner": name, "count": 0, "status": "empty_list"})
            continue

        # Step 2 — Fetch email addresses for logging
        contacts = get_contact_emails(record_ids)
        for rid, info in contacts.items():
            log(f"  Recipient: {info['name']} <{info['email']}>")

        # Step 3 — Remove all contacts from list
        log(f"  Removing {len(record_ids)} contact(s) from list...")
        remove_ok = remove_from_list(list_id, record_ids)

        if not remove_ok:
            log("  Failed to remove contacts — skipping re-add", "ERROR")
            results.append({"partner": name, "count": len(record_ids), "status": "error"})
            continue

        # Brief pause to ensure HubSpot registers the removal
        if not DRY_RUN:
            log("  Waiting 3 seconds before re-adding...")
            time.sleep(3)

        # Step 4 — Re-add all contacts to list
        # This membership change triggers the workflow for each contact
        log(f"  Re-adding {len(record_ids)} contact(s) to trigger workflow...")
        add_ok = add_to_list(list_id, record_ids)

        if add_ok:
            log(f"  Workflow will fire for {len(record_ids)} recipient(s)")
            results.append({"partner": name, "count": len(record_ids), "status": "ok"})
        else:
            log("  Re-add failed — contacts removed but NOT re-added", "ERROR")
            log("  URGENT: Manually re-add contacts to the list in HubSpot", "ERROR")
            results.append({"partner": name, "count": len(record_ids), "status": "error"})

        time.sleep(1.0)

    # ── Summary ──────────────────────────────────────────────────────
    log("\n" + "=" * 55)
    log("SEND SUMMARY")
    col_p = 24; col_c = 9; col_s = 12
    sep = "-" * (col_p + col_c + col_s + 4)
    log(f"{'Partner':<{col_p}}  {'Recipients':>{col_c}}  {'Status':<{col_s}}")
    log(sep)
    any_fail = False
    for r in results:
        log(f"{r['partner']:<{col_p}}  {str(r['count']):>{col_c}}  {r['status']:<{col_s}}")
        if r["status"] == "error": any_fail = True
    log(sep)

    if any_fail:
        log("Errors occurred — check logs above.", "WARN")
        sys.exit(1)
    log("All done. HubSpot workflows triggered for all recipients.")

if __name__ == "__main__":
    main()
