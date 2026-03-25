"""
AMZ Prep — Weekly Lead Report Template Auto-Updater
====================================================
Runs via GitHub Actions every Sunday 11PM EST.
Fetches live contact data from HubSpot, regenerates HubL
template variable blocks, and pushes updates to Design Manager.

Scalable to any number of partners via partners.json config.
"""

import os
import sys
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Configuration ─────────────────────────────────────────────────
HUBSPOT_TOKEN   = os.environ.get("HUBSPOT_TOKEN", "")
SLACK_WEBHOOK   = os.environ.get("SLACK_WEBHOOK_URL", "")
PARTNER_FILTER  = os.environ.get("PARTNER_FILTER", "").strip()
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() == "true"
HS_BASE_URL     = "https://api.hubapi.com"

# HubSpot pipeline stage IDs → display labels
STAGE_LABELS = {
    "13390264":   "Closed Won",
    "13390265":   "Closed Lost",
    "201513064":  "Analysis",
    "1247679147": "Negotiation",
    "1271308871": "Deal Referred",
    "1271308872": "Partner Won",
    "closedwon":  "Closed Won",
    "closedlost": "Closed Lost",
}

# ── Logging ────────────────────────────────────────────────────────
log_entries = []

def log(msg, level="INFO"):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry = {"time": ts, "level": level, "message": msg}
    log_entries.append(entry)
    prefix = {"INFO": "✅", "WARN": "⚠️", "ERROR": "❌", "DEBUG": "🔍"}.get(level, "  ")
    print(f"{prefix} [{ts}] {msg}")

def save_log():
    with open("update_log.json", "w") as f:
        json.dump(log_entries, f, indent=2)

# ── HTTP Helpers ───────────────────────────────────────────────────
def hs_request(method, path, body=None, is_multipart=False):
    """Make a HubSpot API request. Returns (status_code, response_dict)."""
    url = f"{HS_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    if is_multipart:
        # multipart/form-data for CMS PUT file upload
        boundary = "----FormBoundary7MA4YWxkTrZu0gW"
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        data = (
            f"--{boundary}\r\n"
            f"Content-Disposition: form-data; name=\"file\"; filename=\"template.html\"\r\n"
            f"Content-Type: text/html\r\n\r\n"
            f"{body}"
            f"\r\n--{boundary}--\r\n"
        ).encode("utf-8")
    elif body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
    else:
        data = None

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            resp_body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(resp_body)
            except Exception:
                return resp.status, {"raw": resp_body}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8")
        log(f"HTTP {e.code} on {method} {path}: {body_text[:300]}", "ERROR")
        return e.code, {"error": body_text}

def slack_notify(msg):
    """Send a Slack message via webhook."""
    if not SLACK_WEBHOOK:
        log("No Slack webhook configured — skipping notification", "WARN")
        return
    data = json.dumps({"text": msg}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK, data=data,
        headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        urllib.request.urlopen(req)
        log("Slack notification sent")
    except Exception as e:
        log(f"Slack notification failed: {e}", "WARN")

# ── HubSpot Data Fetchers ──────────────────────────────────────────
def fetch_contacts(partner_name):
    """Fetch all contacts for a referral partner, sorted by createdate ASC."""
    log(f"Fetching contacts for: {partner_name}")
    all_contacts = []
    offset = 0

    while True:
        body = {
            "filterGroups": [{"filters": [{
                "propertyName": "referral_partner_name",
                "operator": "EQ",
                "value": partner_name
            }]}],
            "properties": [
                "firstname", "lastname", "email", "company",
                "hs_lead_status", "num_associated_deals"
            ],
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
            "limit": 100,
            "after": offset if offset > 0 else None
        }
        if body["after"] is None:
            del body["after"]

        status, resp = hs_request("POST", "/crm/v3/objects/contacts/search", body)

        if status != 200:
            log(f"Contacts API failed for {partner_name}: {status}", "ERROR")
            return None, 0

        results = resp.get("results", [])
        all_contacts.extend(results)
        total = resp.get("total", len(results))

        paging = resp.get("paging", {})
        next_page = paging.get("next", {}).get("after")
        if not next_page or len(all_contacts) >= total:
            break
        offset = next_page
        time.sleep(0.2)  # Rate limit buffer

    log(f"Found {len(all_contacts)} contacts (total in HS: {total}) for {partner_name}")
    return all_contacts, total


def fetch_deal_id(contact_id):
    """Get the first associated deal ID for a contact."""
    status, resp = hs_request(
        "GET",
        f"/crm/v3/objects/contacts/{contact_id}/associations/deals"
    )
    if status == 200:
        results = resp.get("results", [])
        if results:
            return str(results[0].get("id", ""))
    return None


# ── Template Generator ─────────────────────────────────────────────
def get_next_monday_date():
    """Returns the upcoming Monday date string e.g. 'March 30, 2026'"""
    from datetime import timedelta
    now_utc = datetime.now(timezone.utc)
    days_ahead = (0 - now_utc.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (now_utc + timedelta(days=days_ahead)).strftime("%B %d, %Y")


def generate_variable_block(partner_name, contacts, total_count):
    """
    Generate the HubL variable block (top section of template).
    Uses hardcoded contact data + live crm_object calls for UNIQUE deals only.
    Stays well under HubSpot's 10 crm_object() call limit per template.
    """
    today     = datetime.now(timezone.utc).strftime("%B %d, %Y")
    send_date = get_next_monday_date()

    # Build unique deal map — ACTIVE deals only (skip closed won/lost)
    # Closed deals don't change stage so no need for live crm_object calls
    # This keeps us under HubSpot's 10 crm_object() limit per template
    SKIP_CLOSED = True  # Set False to include all deals (may exceed limit)
    unique_deals = {}
    for c in contacts:
        did = c.get("deal_id")
        if did and did not in unique_deals:
            unique_deals[did] = f"_deal_{did}"

    if SKIP_CLOSED and len(unique_deals) > 10:
        # Too many — fetch deal stages to filter out closed ones
        log(f"  {len(unique_deals)} deals found — filtering to active only to stay under limit", "DEBUG")
        active_deals = {}
        for did, var in unique_deals.items():
            ds_status, ds_resp = hs_request("GET", f"/crm/v3/objects/deals/{did}?properties=dealstage")
            if ds_status == 200:
                stage = ds_resp.get("properties", {}).get("dealstage", "")
                if stage not in {"13390264","13390265","closedwon","closedlost","1271308873","1037017710","1037017711"}:
                    active_deals[did] = var
                    log(f"  Keeping active deal {did} (stage: {stage})", "DEBUG")
                else:
                    log(f"  Skipping closed deal {did} (stage: {stage})", "DEBUG")
            time.sleep(0.1)
        unique_deals = active_deals

    crm_calls = len(unique_deals)
    log(f"  crm_object calls needed: {crm_calls} active deals (limit=10)", "DEBUG")
    if crm_calls > 10:
        log(f"  WARNING: {crm_calls} still exceeds 10 — will trigger HubSpot error", "WARN")

    crm_calls = len(unique_deals)
    log(f"  crm_object calls needed: {crm_calls} unique deals (limit=10)", "DEBUG")
    if crm_calls > 10:
        log(f"  WARNING: {crm_calls} crm_object calls exceeds HubSpot limit of 10!", "WARN")

    lines = []

    # ── Comment block ─────────────────────────────────────────────
    lines += [
        "<!--",
        '  templateType: "email"',
        '  isAvailableForNewContent: true',
        "-->",
        "{#",
        "  =============================================================",
        f"  AMZ Prep — {partner_name} Weekly Lead Report",
        "  AUTO-UPDATED by GitHub Actions on " + today,
        f"  crm_object calls: {crm_calls} unique deals (contact data hardcoded)",
        "  Contacts:",
    ]
    for c in contacts:
        cid  = c["id"]
        fn   = c["properties"].get("firstname", "") or c["properties"].get("email","")
        ln   = c["properties"].get("lastname", "") or ""
        did  = c.get("deal_id")
        lines.append(f"    {(fn+' '+ln).strip():<28} contact: {cid}  deal: {did or chr(8212)}")
    lines += ["  =============================================================", "#}", ""]

    # ── Hardcoded contact sets (avoids crm_object contact calls) ──
    lines.append("{# ── Contact data hardcoded — GitHub Actions updates weekly ── #}")
    for i, c in enumerate(contacts, 1):
        fn = (c["properties"].get("firstname") or "").replace('"', '\"')
        ln = (c["properties"].get("lastname")  or "").replace('"', '\"')
        em = (c["properties"].get("email")     or "").replace('"', '\"')
        co = (c["properties"].get("company")   or "").replace('"', '\"')
        st = (c["properties"].get("hs_lead_status") or "NEW").replace('"', '\"')
        lines.append(
            '{%' + f' set c{i} = ' + '{"' + f'firstname": "{fn}", "lastname": "{ln}", ' +
            f'"email": "{em}", "company": "{co}", "hs_lead_status": "{st}"' + '} %}'
        )
    lines.append("")

    # ── Live deal crm_object calls (unique deals only) ─────────────
    lines.append("{# ── Live deal data — unique deal IDs only, fetched at send time ── #}")
    for did, var in unique_deals.items():
        lines.append(
            '{%' + f' set {var} = crm_object("deal", "{did}", ' +
            '"dealname,dealstage,amount,createdate,closedate") %}'
        )
    if not unique_deals:
        lines.append("{# No active deals this period #}")
    lines.append("")

    # ── Assign deal variable per contact (reuse if shared deal) ───
    lines.append("{# ── Assign deal per contact (null if no deal) ── #}")
    for i, c in enumerate(contacts, 1):
        did = c.get("deal_id")
        var = unique_deals.get(did, "null") if did else "null"
        lines.append('{%' + f' set d{i} = {var} ' + '%}')
    lines.append("")

    # ── Normalised lead statuses ───────────────────────────────────
    lines.append("{# ── Normalised lead statuses ── #}")
    for i in range(1, len(contacts) + 1):
        lines.append('{%' + f' set s{i} = c{i}.hs_lead_status | lower ' + '%}')
    lines.append("")

    # ── Counts ────────────────────────────────────────────────────
    lines += [
        "{# ── Counts ── #}",
        '{%' + f' set rpt_total = {total_count} ' + '%}',
        "",
        '{%' + " set rpt_connected = 0 " + "%}",
    ]
    for i in range(1, len(contacts) + 1):
        lines.append(
            '{%' + f' if s{i} == "connected" ' + '%}' +
            '{%' + ' set rpt_connected = rpt_connected + 1 ' + '%}' +
            '{%' + ' endif ' + '%}'
        )

    lines += ["", "{# open = deal NOT closed #}", '{%' + " set rpt_active_deals = 0 " + "%}"]
    for i, c in enumerate(contacts, 1):
        if c.get("deal_id"):
            lines.append(
                '{%' + f' if d{i} and d{i}.dealstage and d{i}.dealstage != "13390264" ' +
                f'and d{i}.dealstage != "13390265" and d{i}.dealstage != "closedwon" ' +
                f'and d{i}.dealstage != "closedlost" ' + '%}' +
                '{%' + ' set rpt_active_deals = rpt_active_deals + 1 ' + '%}' +
                '{%' + ' endif ' + '%}'
            )

    lines += ["", "{# closed won #}", '{%' + " set rpt_won = 0 " + "%}"]
    for i, c in enumerate(contacts, 1):
        if c.get("deal_id"):
            lines.append(
                '{%' + f' if d{i} and (d{i}.dealstage == "13390264" ' +
                f'or d{i}.dealstage == "closedwon" or d{i}.dealstage == "1271308872") ' + '%}' +
                '{%' + ' set rpt_won = rpt_won + 1 ' + '%}' +
                '{%' + ' endif ' + '%}'
            )

    lines += [
        "",
        '{%' + f' set report_date = "{send_date}" ' + '%}',
        "",
        '{%' + ' set pairs = [',
    ]
    for i in range(1, len(contacts) + 1):
        comma = "," if i < len(contacts) else ""
        lines.append(f"  [c{i}, d{i}, s{i}]{comma}")
    lines.append("] %}")
    lines.append("")

    return "\n".join(lines)


def regenerate_template(current_template, partner_name, contacts, total_count):
    """
    Replace ONLY the variable block at the top of the template.
    Everything from the Stage label macro onwards is kept VERBATIM.
    """
    split_marker = "{# ── Stage label macro"
    idx = current_template.find(split_marker)

    if idx == -1:
        log("Could not find split marker in template — aborting regeneration", "ERROR")
        return None

    preserved_section = current_template[idx:]
    new_block = generate_variable_block(partner_name, contacts, total_count)
    return new_block + preserved_section


# ── Design Manager API ─────────────────────────────────────────────
def read_template(filename):
    """Download current template from HubSpot Design Manager."""
    encoded = urllib.parse.quote(filename)
    status, resp = hs_request("GET", f"/cms/v3/source-code/draft/content/{encoded}")

    if status == 200:
        # HubSpot returns the file content in the 'source' field
        return resp.get("source", resp.get("raw", ""))
    else:
        log(f"Failed to read template '{filename}': HTTP {status}", "ERROR")
        return None


def write_template(filename, content):
    """Upload updated template directly to LIVE in HubSpot Design Manager.
    Writing to live bypasses the draft->push-to-live step entirely.
    """
    if DRY_RUN:
        log(f"DRY RUN — would write {len(content)} chars to '{filename}'", "WARN")
        return True

    encoded = urllib.parse.quote(filename)
    # Write directly to live — no push-to-live step needed
    status, resp = hs_request(
        "PUT",
        f"/cms/v3/source-code/published/content/{encoded}",
        body=content,
        is_multipart=True
    )

    if status in (200, 201):
        log(f"Template written to published: '{filename}' ✅")
        return True
    else:
        log(f"Failed to write template '{filename}': HTTP {status}", "ERROR")
        return False


def publish_templates(filenames):
    """No-op: templates are now written directly to live, no push step needed."""
    log(f"Templates written directly to live — no publish step needed ✅")
    return True


# ── Contact ID Extractor ───────────────────────────────────────────
def extract_contact_ids_from_template(template_content):
    """
    Extract existing contact IDs from the current template.
    Looks for: crm_object("contact", "ID",
    Returns a set of ID strings.
    """
    import re
    pattern = r'crm_object\("contact",\s*"(\d+)"'
    return set(re.findall(pattern, template_content))


# ── Main Orchestrator ──────────────────────────────────────────────
def process_partner(partner):
    """Process one partner: fetch, compare, regenerate, push."""
    name     = partner["partner_name"]
    filename = partner["template_file"]
    result   = {
        "partner": name,
        "status": "ok",
        "contacts_found": 0,
        "changed": False,
        "pushed": False,
        "error": None,
    }

    log(f"─── Processing: {name} ───")

    # Phase A — Fetch live contacts
    contacts, total = fetch_contacts(name)
    if contacts is None:
        result["status"] = "error"
        result["error"] = "Failed to fetch contacts from HubSpot"
        return result

    if total == 0:
        log(f"SAFETY: 0 contacts returned for {name} — skipping to prevent data loss", "WARN")
        result["status"] = "skipped"
        result["error"] = "Zero contacts returned — possible API filter issue"
        return result

    result["contacts_found"] = total

    # Fetch deal IDs for contacts — only needed to check deal count vs template
    # We fetch all but will only use crm_object for ACTIVE (non-closed) deals
    CLOSED_STAGES = {"13390264", "13390265", "closedwon", "closedlost",
                     "1271308873"}  # closed lost in affiliate pipeline
    for c in contacts:
        num_deals = int(c["properties"].get("num_associated_deals") or 0)
        if num_deals > 0:
            did = fetch_deal_id(c["id"])
            c["deal_id"] = did
            log(f"  Deal ID for {c['id']}: {did}")
            time.sleep(0.1)
        else:
            c["deal_id"] = None

    # Phase B — Read current template
    log(f"Reading current template: {filename}")
    current_template = read_template(filename)
    if current_template is None:
        result["status"] = "error"
        result["error"] = "Failed to read current template from Design Manager"
        return result

    # Phase C — Detect changes
    current_ids = extract_contact_ids_from_template(current_template)
    live_ids    = {c["id"] for c in contacts}
    added       = live_ids - current_ids
    removed     = current_ids - live_ids

    if added:
        log(f"  New contacts to add: {added}")
    if removed:
        log(f"  Contacts to remove: {removed}")

    # Also check if rpt_total count differs
    import re
    total_match = re.search(r'rpt_total\s*=\s*(\d+)', current_template)
    current_total = int(total_match.group(1)) if total_match else -1
    count_changed = (current_total != total)

    if count_changed:
        log(f"  rpt_total mismatch: template has {current_total}, HubSpot has {total}")

    # Also check if any contact status changed (hardcoded in template)
    import re as _re
    status_changed = False
    for c in contacts:
        cid = c["id"]
        live_status = (c["properties"].get("hs_lead_status") or "NEW").replace('"', '\"')
        # Look for this contact's hardcoded status in the template
        pattern = f'"hs_lead_status": "[^"]*".*?{cid}|{cid}.*?"hs_lead_status": "[^"]*"'
        # Simpler: check if live status appears near contact ID
        block_match = _re.search(
            r'set c\d+ = {.*?"hs_lead_status":\s*"([^"]*)".*?}',
            current_template
        )
        # Check overall: if template has old status string vs live status
        old_status_pattern = f'"hs_lead_status": "Active Client"'
        if live_status == "CW" and old_status_pattern.replace("Active Client", "Active Client") in current_template:
            pass  # will be caught by full regeneration

    # Trigger regeneration if any contact's status doesn't match template
    template_statuses = set(_re.findall(r'"hs_lead_status":\s*"([^"]+)"', current_template))
    live_statuses = {(c["properties"].get("hs_lead_status") or "NEW") for c in contacts}
    # If live has a status not in template or vice versa — regenerate
    status_mismatch = bool(live_statuses - template_statuses) or bool(template_statuses - live_statuses - {"NEW"})
    if status_mismatch:
        log(f"  Status changes detected — regenerating template")
        status_changed = True

    result["changed"] = bool(added or removed or count_changed or status_changed)

    if not result["changed"]:
        log(f"  No changes detected — template is up to date ✅")
        return result

    # Phase D — Regenerate template
    log(f"  Regenerating template variable block...")
    new_template = regenerate_template(current_template, name, contacts, total)
    if new_template is None:
        result["status"] = "error"
        result["error"] = "Template regeneration failed — split marker not found"
        return result

    # Phase E — Push to Design Manager
    pushed = write_template(filename, new_template)
    result["pushed"] = pushed

    if not pushed:
        result["status"] = "error"
        result["error"] = "Failed to write updated template to Design Manager"
        return result

    log(f"  Template updated successfully: {name} ✅")
    return result


def main():
    log("═══════════════════════════════════════════════════════")
    log("AMZ Prep — Weekly Lead Report Template Auto-Updater")
    log(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        log("MODE: DRY RUN — no changes will be written", "WARN")
    log("═══════════════════════════════════════════════════════")

    # Validate token
    if not HUBSPOT_TOKEN:
        log("HUBSPOT_TOKEN is not set — cannot proceed", "ERROR")
        slack_notify("❌ *Weekly Lead Report Auto-Update FAILED*\nHUBSPOT_TOKEN secret is not configured in GitHub.")
        save_log()
        sys.exit(1)

    # Load partners config
    config_path = os.path.join(os.path.dirname(__file__), "..", "partners.json")
    with open(config_path) as f:
        config = json.load(f)

    partners = [p for p in config["partners"] if p.get("active", True)]

    # Filter to specific partner if requested
    if PARTNER_FILTER:
        partners = [p for p in partners if p["partner_name"] == PARTNER_FILTER]
        if not partners:
            log(f"No active partner found matching: '{PARTNER_FILTER}'", "WARN")

    log(f"Partners to process: {[p['partner_name'] for p in partners]}")

    # Process each partner
    results = []
    files_to_publish = []

    for partner in partners:
        result = process_partner(partner)
        results.append(result)
        if result.get("pushed"):
            files_to_publish.append(partner["template_file"])
        time.sleep(0.5)  # Brief pause between partners

    # Publish all updated files to live in one call
    publish_ok = True
    if files_to_publish and not DRY_RUN:
        log(f"Publishing {len(files_to_publish)} file(s) to live...")
        publish_ok = publish_templates(files_to_publish)

    # Build Slack summary
    any_error = any(r["status"] == "error" for r in results)
    run_date  = datetime.now(timezone.utc).strftime("%A %B %d, %Y at %H:%M UTC")

    if any_error:
        lines = [f"❌ *Weekly Lead Report Auto-Update — ERRORS DETECTED*\n_{run_date}_\n"]
        for r in results:
            icon = "✅" if r["status"] == "ok" else ("⏭️" if r["status"] == "skipped" else "❌")
            changed = "Updated ✅" if r.get("changed") and r.get("pushed") else ("No change" if not r.get("changed") else "FAILED ❌")
            lines.append(f"{icon} *{r['partner']}*: {r['contacts_found']} contacts — {changed}")
            if r.get("error"):
                lines.append(f"   Error: {r['error']}")
        lines.append("\n⚠️ *Action needed: manually check Design Manager before Monday 10AM EST*")
        slack_notify("\n".join(lines))
    else:
        mode_note = " _(dry run — no changes written)_" if DRY_RUN else ""
        lines = [f"✅ *Weekly Lead Report Auto-Update Complete*{mode_note}\n_{run_date}_\n"]
        for r in results:
            icon  = "⏭️" if r["status"] == "skipped" else "✅"
            changed = "Updated ✅" if r.get("changed") and r.get("pushed") else ("No change needed" if not r.get("changed") else "Dry run")
            lines.append(f"{icon} *{r['partner']}*: {r['contacts_found']} contacts — {changed}")
        if not DRY_RUN and files_to_publish:
            lines.append(f"\n📤 {len(files_to_publish)} template(s) published to live")
        lines.append("\n🗓️ Monday 10AM EST Zapier send is ready ✅")
        slack_notify("\n".join(lines))

    # Save run log
    final_log = {
        "run_time": datetime.now(timezone.utc).isoformat(),
        "dry_run": DRY_RUN,
        "partner_filter": PARTNER_FILTER or "all",
        "results": results,
        "files_published": files_to_publish,
        "publish_success": publish_ok,
        "log": log_entries,
    }
    with open("update_log.json", "w") as f:
        json.dump(final_log, f, indent=2)

    # Exit with error code if any partner failed
    if any_error or not publish_ok:
        log("One or more partners failed — exiting with error", "ERROR")
        sys.exit(1)

    log("All partners processed successfully ✅")


if __name__ == "__main__":
    main()
