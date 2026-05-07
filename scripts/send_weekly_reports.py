"""
AMZ Prep — Weekly Lead Report Email Sender
==========================================
Replaces Zapier. Runs every Monday at 2:45 PM UTC (9:45 AM EST).

For each active partner:
  1. Fetches all contacts from the partner's HubSpot recipient list
  2. Combines them with global CC contacts and the partner's assigned AE CC contact
  3. Removes all from the list, then immediately re-adds them
  4. HubSpot detects the re-membership → triggers the workflow for each contact
  5. Every recipient gets the exact same fully-rendered Design Manager template

TEST_CC_ONLY mode (set via GitHub Actions input or env var):
  When true — skips actual partner list members entirely.
  Only enrolls the global + per-partner CC contacts in the workflow.
  Use this to verify the CC feature and template rendering with internal
  team before enabling for real partner sends.

CC logic (two levels — both defined in partners.json):
  Global cc_contact_ids  — top-level, enrolled in every partner's workflow send
  Partner cc_contact_ids — per-partner, only for that partner's email (assigned AE)

Required GitHub Secrets:
  HUBSPOT_TOKEN    — HubSpot Private App token (crm.lists.read + crm.lists.write)

Required partners.json fields:
  hubspot_list_id     — HubSpot Static List ID
  hubspot_workflow_id — for reference/logging only
  active              — only processes active: true partners
  cc_contact_ids      — (optional) per-partner AE contact IDs

Top-level partners.json fields:
  cc_contact_ids      — global contact IDs to CC on every partner email
"""

import os, sys, json, time, urllib.request, urllib.error
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────
HUBSPOT_TOKEN  = os.environ.get("HUBSPOT_TOKEN", "")
PARTNER_FILTER = os.environ.get("PARTNER_FILTER", "").strip()
DRY_RUN        = os.environ.get("DRY_RUN", "false").lower() == "true"
TEST_CC_ONLY   = os.environ.get("TEST_CC_ONLY", "false").lower() == "true"
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
    """Add contacts to a HubSpot list — triggers the workflow for each."""
    if DRY_RUN:
        log(f"  DRY RUN — would add {len(record_ids)} contact(s) to list {list_id}", "DEBUG")
        return True
    status, resp = hs("PUT",
        f"/crm/v3/lists/{list_id}/memberships/add-and-remove",
        {"recordIdsToAdd": record_ids, "recordIdsToRemove": []}
    )
    if status == 200:
        added = resp.get("recordIdsAdded", [])
        log(f"  Re-added {len(added)} contact(s) — workflow triggered for each")
        return True
    log(f"  Add to list failed: HTTP {status}", "WARN")
    return False

# ── Main ─────────────────────────────────────────────────────────────
def main():
    log("=" * 60)
    log("AMZ Prep — Weekly Lead Report Email Sender")
    log(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        log("MODE: DRY RUN — no list changes will be made", "WARN")
    if TEST_CC_ONLY:
        log("MODE: TEST CC ONLY — real partner contacts skipped, CC team only", "WARN")
    log("=" * 60)

    if not HUBSPOT_TOKEN:
        log("HUBSPOT_TOKEN not set", "ERROR"); sys.exit(1)

    config_path = os.path.join(os.path.dirname(__file__), "..", "partners.json")
    with open(config_path) as f:
        config = json.load(f)

    partners = [p for p in config["partners"] if p.get("active", True)]
    if PARTNER_FILTER:
        names    = [x.strip() for x in PARTNER_FILTER.split(",")]
        partners = [p for p in partners if p["partner_name"] in names]

    # ── Load global CC contact IDs ──────────────────────────────────
    global_cc_ids = [int(cid) for cid in config.get("cc_contact_ids", [])]
    if global_cc_ids:
        log(f"Global CC: {len(global_cc_ids)} contact(s) receive every partner email")

    # Only process partners with list configured
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

        # Per-partner CC (assigned AE) — empty during test phase
        partner_cc_ids = [int(cid) for cid in partner.get("cc_contact_ids", [])]

        # Deduplicated CC list for this partner
        all_cc_ids = list(set(global_cc_ids + partner_cc_ids))

        log(f"\n--- {name} (List: {list_id}) ---")

        if TEST_CC_ONLY:
            # ── TEST MODE: skip real partner contacts ────────────────
            # Only enroll the CC contacts so the internal team can verify
            # the template renders correctly before any partner receives it.
            log(f"  TEST CC ONLY — skipping real list members")
            log(f"  Sending to {len(all_cc_ids)} CC contact(s) only")

            if not all_cc_ids:
                log("  No CC contacts configured — skipping", "WARN")
                results.append({"partner": name, "count": 0, "cc_count": 0, "status": "skipped"})
                continue

            # Fetch names for logging
            cc_info = get_contact_emails(all_cc_ids)
            for rid, info in cc_info.items():
                log(f"  Test recipient: {info['name']} <{info['email']}>")

            # Remove CC contacts from list (in case they're already in it)
            log(f"  Removing {len(all_cc_ids)} CC contact(s) from list...")
            remove_ok = remove_from_list(list_id, all_cc_ids)
            if not remove_ok:
                log("  Remove failed — skipping", "ERROR")
                results.append({"partner": name, "count": 0, "cc_count": len(all_cc_ids), "status": "error"})
                continue

            if not DRY_RUN:
                log("  Waiting 3 seconds...")
                time.sleep(3)

            # Re-add CC contacts only → workflow fires → they get the template
            log(f"  Re-adding {len(all_cc_ids)} CC contact(s) to trigger workflow...")
            add_ok = add_to_list(list_id, all_cc_ids)

            if add_ok:
                log(f"  Test send complete — {len(all_cc_ids)} internal recipient(s) will receive {name} template")
                results.append({"partner": name, "count": 0, "cc_count": len(all_cc_ids), "status": "ok_test"})
            else:
                log("  Re-add failed", "ERROR")
                results.append({"partner": name, "count": 0, "cc_count": len(all_cc_ids), "status": "error"})

        else:
            # ── LIVE MODE: real partner contacts + CC contacts ───────
            # Step 1 — Get current list members
            record_ids = get_list_members(list_id)
            log(f"  {len(record_ids)} partner recipient(s) in list")

            if not record_ids:
                log("  List is empty — skipping", "WARN")
                results.append({"partner": name, "count": 0, "cc_count": len(all_cc_ids), "status": "empty_list"})
                continue

            # Log partner recipients
            contacts = get_contact_emails(record_ids)
            for rid, info in contacts.items():
                log(f"  Partner recipient: {info['name']} <{info['email']}>")

            # Combine partner + CC (deduped)
            all_record_ids = list(set(record_ids + all_cc_ids))
            log(f"  Total for send: {len(all_record_ids)} "
                f"({len(record_ids)} partner + {len(set(all_cc_ids) - set(record_ids))} new CC)")

            # Step 3 — Remove all
            log(f"  Removing {len(all_record_ids)} contact(s)...")
            remove_ok = remove_from_list(list_id, all_record_ids)
            if not remove_ok:
                log("  Remove failed — skipping re-add", "ERROR")
                results.append({"partner": name, "count": len(record_ids), "cc_count": len(all_cc_ids), "status": "error"})
                continue

            if not DRY_RUN:
                log("  Waiting 3 seconds before re-adding...")
                time.sleep(3)

            # Step 4 — Re-add all → workflow fires for all
            log(f"  Re-adding {len(all_record_ids)} contact(s)...")
            add_ok = add_to_list(list_id, all_record_ids)

            if add_ok:
                log(f"  Workflow triggered for {len(all_record_ids)} recipient(s) "
                    f"({len(record_ids)} partner + {len(all_cc_ids)} CC)")
                results.append({"partner": name, "count": len(record_ids), "cc_count": len(all_cc_ids), "status": "ok"})
            else:
                log("  Re-add failed — contacts removed but NOT re-added", "ERROR")
                log("  URGENT: Manually re-add contacts in HubSpot", "ERROR")
                results.append({"partner": name, "count": len(record_ids), "cc_count": len(all_cc_ids), "status": "error"})

        time.sleep(1.0)

    # ── Summary ──────────────────────────────────────────────────────
    log("\n" + "=" * 60)
    log("SEND SUMMARY" + (" — TEST CC ONLY MODE" if TEST_CC_ONLY else ""))
    col_p = 24; col_c = 11; col_cc = 12; col_s = 16
    sep = "-" * (col_p + col_c + col_cc + col_s + 6)
    log(f"{'Partner':<{col_p}}  {'Recipients':>{col_c}}  {'CC Contacts':>{col_cc}}  {'Status':<{col_s}}")
    log(sep)
    any_fail = False
    for r in results:
        if r["status"] == "error":
            status_str = "FAILED"
            any_fail   = True
        elif r["status"] == "ok_test":
            status_str = "Test sent ✓"
        elif r["status"] == "ok":
            status_str = "Sent ✓"
        elif r["status"] == "empty_list":
            status_str = "Empty list"
        else:
            status_str = r["status"]
        log(
            f"{r['partner']:<{col_p}}  "
            f"{str(r['count']):>{col_c}}  "
            f"{str(r['cc_count']):>{col_cc}}  "
            f"{status_str:<{col_s}}"
        )
    log(sep)
    if TEST_CC_ONLY:
        log("Test complete. Verify template in inboxes, then re-run without TEST_CC_ONLY for live send.")
    else:
        log(f"Global CC ({len(global_cc_ids)} contact(s)) received all partner emails.")
    if any_fail:
        log("Errors occurred — check logs above.", "WARN")
        sys.exit(1)
    log("Done.")

if __name__ == "__main__":
    main()
