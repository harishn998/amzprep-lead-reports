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
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
PARTNER_FILTER  = os.environ.get("PARTNER_FILTER", "").strip()
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() == "true"
HS_BASE_URL     = "https://api.hubapi.com"
SLACK_API       = "https://slack.com/api"
NOTIFY_CHANNEL  = "C0APUEEFC30"   # #tech-feature-testing

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
    prefix = {"INFO": "[INFO]", "WARN": "[WARN]", "ERROR": "[ERROR]", "DEBUG": "[DEBUG]"}.get(level, "      ")
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
        # IMPORTANT: use string concatenation NOT f-strings here.
        # The template body contains {%...%} and {{...}} HubL tags —
        # putting `body` inside an f-string would try to evaluate those
        # as Python format expressions and corrupt the payload.
        boundary = "----FormBoundary7MA4YWxkTrZu0gW"
        headers["Content-Type"] = "multipart/form-data; boundary=" + boundary
        data = (
            "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"file\"; filename=\"template.html\"\r\n"
            "Content-Type: text/html\r\n\r\n"
            + body +
            "\r\n--" + boundary + "--\r\n"
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
    """Post a message to #tech-feature-testing via Bot Token."""
    if not SLACK_BOT_TOKEN:
        log("SLACK_BOT_TOKEN not configured — skipping notification", "WARN")
        return
    data = json.dumps({"channel": NOTIFY_CHANNEL, "text": msg}).encode("utf-8")
    req  = urllib.request.Request(
        f"{SLACK_API}/chat.postMessage", data=data,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        }, method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            if body.get("ok"):
                log("Slack notification sent to #tech-feature-testing")
            else:
                log(f"Slack API error: {body.get('error')}", "WARN")
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
CLOSED_WON_STAGES  = {"13390264", "closedwon", "1271308872"}
CLOSED_LOST_STAGES = {"13390265", "closedlost"}
CHURN_STATUSES     = {"churned customer", "churned"}
WON_CONTACT_STATUS = {"cw", "amzdealwon", "customer"}

def classify_deal(stage):
    """Returns 'active', 'won', 'lost', or 'none'"""
    if stage in CLOSED_WON_STAGES:  return "won"
    if stage in CLOSED_LOST_STAGES: return "lost"
    if stage: return "active"
    return "none"


def get_next_monday_date():
    """Returns the date range string for the report week.
    Script runs Sunday night to update templates for Monday send.
    The report covers the PREVIOUS Monday through the PREVIOUS Sunday.
    Example: script runs Sun Apr 5 → send is Mon Apr 6 → report covers Mar 30 – Apr 5, 2026
    """
    from datetime import timedelta
    now_utc   = datetime.now(timezone.utc)
    # Next Monday = the send date
    days_to_monday = (0 - now_utc.weekday()) % 7
    if days_to_monday == 0:
        days_to_monday = 7
    send_monday   = now_utc + timedelta(days=days_to_monday)
    # Previous week: Monday = 7 days before send_monday, Sunday = 1 day before send_monday
    week_start    = send_monday - timedelta(days=7)   # previous Monday
    week_end      = send_monday - timedelta(days=1)   # previous Sunday
    # Format: "March 30 – April 5, 2026"
    # Use %-d to strip leading zero from day numbers (Linux/GitHub Actions compatible)
    if week_start.month == week_end.month:
        # Same month: "April 6 – 12, 2026"
        return f"{week_start.strftime('%B %-d')} – {week_end.strftime('%-d, %Y')}"
    else:
        # Different months: "March 30 – April 5, 2026"
        return f"{week_start.strftime('%B %-d')} – {week_end.strftime('%B %-d, %Y')}"


def generate_variable_block(partner_name, contacts, total_count):
    """
    Production-ready HubL variable block generator.
    Hardcodes contact data, uses crm_object only for ACTIVE deals,
    includes show_deal/show_won dedup flags in pairs array.
    """
    send_date = get_next_monday_date()
    today     = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Fetch deal stages for contacts that have deals
    for c in contacts:
        did = c.get("deal_id")
        if did and "deal_stage" not in c:
            ds_status, ds_resp = hs_request("GET", "/crm/v3/objects/deals/" + did + "?properties=dealstage")
            c["deal_stage"] = ds_resp.get("properties", {}).get("dealstage", "") if ds_status == 200 else ""
            time.sleep(0.1)
        elif not did:
            c["deal_stage"] = ""

    # Classify each contact's deal
    classified = [classify_deal(c.get("deal_stage", "")) for c in contacts]

    # Build unique ACTIVE deal map (crm_object only for active deals)
    unique_active_deals = {}
    for c, cls in zip(contacts, classified):
        did = c.get("deal_id")
        if did and cls == "active" and did not in unique_active_deals:
            unique_active_deals[did] = "_deal_" + did

    # show_deal flags (first NON-churned/won occurrence of each active deal)
    # Churned/won contacts share deal IDs but must not represent active deal rows —
    # their company names would be wrong (e.g. "Walmart, Inc." instead of "Monument Grills")
    seen_deal_ids = set()
    show_deal_flags = []

    # First pass: determine the correct (non-skip) representative per deal
    deal_winner = {}  # deal_id -> contact index (1-based)
    for idx, (c, cls) in enumerate(zip(contacts, classified), 1):
        did = c.get("deal_id")
        st  = (c["properties"].get("hs_lead_status") or "").lower()
        if did and cls == "active" and did not in deal_winner:
            if st not in CHURN_STATUSES and st not in WON_CONTACT_STATUS:
                deal_winner[did] = idx

    # Second pass: fall back to first contact if all were skipped
    for idx, (c, cls) in enumerate(zip(contacts, classified), 1):
        did = c.get("deal_id")
        if did and cls == "active" and did not in deal_winner:
            deal_winner[did] = idx  # all churned/won — use first anyway

    # Build flags
    for idx, (c, cls) in enumerate(zip(contacts, classified), 1):
        did = c.get("deal_id")
        if did and cls == "active" and deal_winner.get(did) == idx:
            show_deal_flags.append(True)
            seen_deal_ids.add(did)
        else:
            show_deal_flags.append(False)

    # show_won flags (won by status or deal stage, excluding churned)
    seen_won_keys = set()
    show_won_flags = []
    for c, cls in zip(contacts, classified):
        did = c.get("deal_id")
        st  = (c["properties"].get("hs_lead_status") or "").lower()
        is_won = (st in WON_CONTACT_STATUS or cls == "won") and st not in CHURN_STATUSES
        won_key = did if (did and cls == "won") else c["id"]
        if is_won and won_key not in seen_won_keys:
            show_won_flags.append(True)
            seen_won_keys.add(won_key)
        else:
            show_won_flags.append(False)

    # Hardcoded counts from live data
    rpt_connected    = sum(1 for c in contacts if (c["properties"].get("hs_lead_status") or "").lower() == "connected")
    rpt_active_deals = len(seen_deal_ids)
    rpt_won          = sum(show_won_flags)
    crm_calls        = len(unique_active_deals)
    log("  crm_object calls: " + str(crm_calls) + " | active_deals=" + str(rpt_active_deals) + " | won=" + str(rpt_won), "DEBUG")
    if crm_calls > 10:
        log("  WARNING: " + str(crm_calls) + " crm_object calls exceeds HubSpot limit of 10!", "WARN")

    lines = [
        "<!--",
        '  templateType: "email"',
        '  isAvailableForNewContent: true',
        "-->",
        "{#",
        "  =============================================================",
        "  AMZ Prep \u2014 " + partner_name + " Weekly Lead Report",
        "  AUTO-UPDATED by GitHub Actions on " + today,
        "  crm_object: " + str(crm_calls) + " | ActiveDeals=" + str(rpt_active_deals) + " | Won=" + str(rpt_won),
        "  Contacts:",
    ]
    for c, cls in zip(contacts, classified):
        fn  = c["properties"].get("firstname", "") or c["properties"].get("email", "")
        ln  = c["properties"].get("lastname",  "") or ""
        did = c.get("deal_id") or "\u2014"
        lines.append("    " + (fn + " " + ln).strip().ljust(28) + "  contact: " + c["id"] + "  deal: " + str(did) + " [" + cls + "]")
    lines += ["  =============================================================", "#}", ""]

    # Hardcoded contact data as HubL dicts — accessed via c.firstname, c.company etc.
    lines.append("{# \u2500\u2500 Contact data hardcoded \u2014 GitHub Actions updates weekly \u2500\u2500 #}")
    for i, c in enumerate(contacts, 1):
        fn = (c["properties"].get("firstname") or "").replace('"', '\\"')
        ln = (c["properties"].get("lastname")  or "").replace('"', '\\"')
        em = (c["properties"].get("email")     or "").replace('"', '\\"')
        co = (c["properties"].get("company")   or "").replace('"', '\\"')
        st = (c["properties"].get("hs_lead_status") or "NEW").replace('"', '\\"')
        # Build the dict literal cleanly — no HS/HE tricks that cause double-quote bugs
        lines.append(
            "{% set c" + str(i) + " = {"
            + '"firstname": "' + fn + '", '
            + '"lastname": "' + ln + '", '
            + '"email": "' + em + '", '
            + '"company": "' + co + '", '
            + '"hs_lead_status": "' + st + '"'
            + "} %}"
        )
    lines.append("")

    # Live deal crm_object calls
    lines.append("{# \u2500\u2500 Live deal data \u2014 active deals only, fetched at send time \u2500\u2500 #}")
    for did, var in unique_active_deals.items():
        lines.append(
            '{% set ' + var + ' = crm_object("deal", "' + did + '", ' +
            '"dealname,dealstage,amount,createdate,closedate") %}'
        )
    if not unique_active_deals:
        lines.append("{# No active deals this period #}")
    lines.append("")

    # Assign deal per contact
    lines.append("{# \u2500\u2500 Assign deal per contact (null if closed/no deal) \u2500\u2500 #}")
    for i, (c, cls) in enumerate(zip(contacts, classified), 1):
        did = c.get("deal_id")
        var = unique_active_deals.get(did, "null") if (did and cls == "active") else "null"
        lines.append("{% set d" + str(i) + " = " + var + " %}")
    lines.append("")

    # ── KEY FIX: s{i} as explicit lowercase strings ────────────────────────
    # HubSpot HubL silently returns empty for c{i}.hs_lead_status in loops 2+
    # Hardcoding s{i} guarantees correct badge rendering in ALL 4 tables
    lines.append("{# \u2500\u2500 Lead statuses \u2014 explicit strings (avoids HubL dict-access quirk) \u2500\u2500 #}")
    for i, c in enumerate(contacts, 1):
        st_lower = (c["properties"].get("hs_lead_status") or "new").lower().replace('"', '\\"')
        lines.append('{% set s' + str(i) + ' = "' + st_lower + '" %}')
    lines.append("")
    # Dedup flags
    lines.append("{# \u2500\u2500 Dedup: show_deal=first active deal; show_won=first won contact \u2500\u2500 #}")
    for i, (sd, sw) in enumerate(zip(show_deal_flags, show_won_flags), 1):
        lines.append("{% set show_deal" + str(i) + " = " + ("true" if sd else "false") + " %}")
        lines.append("{% set show_won"  + str(i) + " = " + ("true" if sw else "false") + " %}")
    lines.append("")

    # Build won deal name lookup:
    # - If won contact has an active deal with a name → use deal name
    # - If won contact has no deal (e.g. CW status only) → use full contact name
    #   (avoids HubSpot HubL dict access quirks when reading c.firstname from pair[0])
    won_deal_names = {}
    for c, cls, sw in zip(contacts, classified, show_won_flags):
        if sw:
            fn = (c["properties"].get("firstname") or "").strip()
            ln = (c["properties"].get("lastname")  or "").strip()
            full_name = (fn + " " + ln).strip() or (c["properties"].get("email") or "")
            if c.get("deal_id") and cls == "won":
                ds_status, ds_resp = hs_request("GET", "/crm/v3/objects/deals/" + c["deal_id"] + "?properties=dealname")
                if ds_status == 200:
                    deal_name = (ds_resp.get("properties", {}).get("dealname") or "").strip()
                    won_deal_names[c["id"]] = deal_name if deal_name else full_name
                else:
                    won_deal_names[c["id"]] = full_name
                time.sleep(0.1)
            else:
                # No active deal — use contact name as the identifier
                won_deal_names[c["id"]] = full_name

    # Hardcoded counts
    lines += [
        "{# \u2500\u2500 Counts \u2500\u2500 #}",
        "{% set rpt_total = "        + str(total_count)    + " %}",
        "{% set rpt_connected = "    + str(rpt_connected)   + " %}",
        "{% set rpt_active_deals = " + str(rpt_active_deals)+ " %}",
        "{% set rpt_won = "          + str(rpt_won)         + " %}",
        "",
        '{% set report_date = "' + send_date + '" %}',
        "",
        "{% set pairs = [",
    ]
    # ── 8-element pairs ─────────────────────────────────────────────────────
    # pair[0]=c  pair[1]=d  pair[2]=s  pair[3]=show_deal  pair[4]=show_won
    # pair[5]=won display name  pair[6]=won company  pair[7]=company (all tables)
    for i, (c, sw) in enumerate(zip(contacts, show_won_flags), 1):
        comma = "," if i < len(contacts) else ""
        won_name    = won_deal_names.get(c["id"], "").replace('"', '\\"').strip()
        won_company = (c["properties"].get("company") or "").replace('"', '\\"').strip() if sw else ""
        company     = (c["properties"].get("company") or "").replace('"', '\\"').strip()
        lines.append(
            "  [c" + str(i) + ", d" + str(i) + ", s" + str(i) +
            ", show_deal" + str(i) + ", show_won" + str(i) +
            ', "' + won_name    + '"'
            ', "' + won_company + '"'
            ', "' + company     + '"]' + comma
        )
    lines += ["] %}", ""]

    return "\n".join(lines)

def regenerate_template(current_template, partner_name, contacts, total_count):
    """
    Replace ONLY the variable block at the top of the template.
    Everything from the Stage label macro onwards is kept VERBATIM,
    with one exception: Table 3 Deal Name cell is auto-corrected if wrong.
    """
    split_marker = "{# ── Stage label macro"
    idx = current_template.find(split_marker)

    if idx == -1:
        log("Could not find split marker in template — aborting regeneration", "ERROR")
        return None

    preserved_section = current_template[idx:]

    # ── Auto-correct Table 3 Deal Name cell ────────────────────────────────
    # Table 3 (Deal Status) must show d.dealname — never the pair[5]/contact fallback.
    # pair[5] is for Table 4 (Closed Won) only where d is null.
    WRONG_T3_DEALNAME = (
        '{% if pair[5] %}{{ pair[5] }}{% elif c.firstname or c.lastname %}'
        '{{ c.firstname }} {{ c.lastname }}{% else %}{{ c.email }}{% endif %}'
    )
    CORRECT_T3_DEALNAME = '{{ d.dealname if d.dealname else "\u2014" }}'

    if WRONG_T3_DEALNAME in preserved_section:
        preserved_section = preserved_section.replace(WRONG_T3_DEALNAME, CORRECT_T3_DEALNAME, 1)
        log("  Auto-corrected Table 3 Deal Name cell", "DEBUG")

    # Auto-correct Closed Won table to use pair[5]/pair[6] without dict access
    WRONG_WON_NAME = (
        '{% if pair[5] %}{{ pair[5] }}{% elif c.firstname or c.lastname %}'
        '{{ c.firstname }} {{ c.lastname }}{% else %}{{ c.email }}{% endif %}'
    )
    CORRECT_WON_NAME = '{{ pair[5] if pair[5] else c.email }}'
    if WRONG_WON_NAME in preserved_section:
        preserved_section = preserved_section.replace(WRONG_WON_NAME, CORRECT_WON_NAME)
        log("  Auto-corrected Closed Won name cell", "DEBUG")

    # ── Auto-correct c.company → pair[7] in ALL table cells ──────────────────
    # HubSpot HubL silently returns empty for c.company dict access in loops 2+
    # pair[7] is always pre-populated with the company from HubSpot
    for wrong_co, right_co in [
        ('{{ c.company if c.company else "\u2014" }}', '{{ pair[7] if pair[7] else "\u2014" }}'),
        ('{{ c.company if c.company else "—" }}',       '{{ pair[7] if pair[7] else "—" }}'),
    ]:
        if wrong_co in preserved_section:
            preserved_section = preserved_section.replace(wrong_co, right_co)
            log("  Auto-corrected c.company → pair[7]", "DEBUG")

    # ── Auto-correct Table 4 Closed Won company cell → pair[6] ─────────────
    # Table 4 is identified by the {% if pair[4] %} loop condition.
    # Within that block, the company cell must use pair[6], not pair[7].
    import re as _re_t4
    # Find the Table 4 won loop block and replace company cell there only
    def fix_table4_company(section):
        # Split at the won table loop start
        won_loop_marker = "{%- if pair[4] %}" if "{%- if pair[4] %}" in section else "{% if pair[4] %}"
        idx = section.find(won_loop_marker)
        if idx == -1:
            return section
        before = section[:idx]
        after  = section[idx:]
        # In the after section, replace first occurrence of pair[7] company cell → pair[6]
        after = after.replace(
            '{{ pair[7] if pair[7] else "\u2014" }}',
            '{{ pair[6] if pair[6] else "\u2014" }}', 1
        )
        after = after.replace(
            '{{ pair[7] if pair[7] else "—" }}',
            '{{ pair[6] if pair[6] else "—" }}', 1
        )
        return before + after
    preserved_section = fix_table4_company(preserved_section)

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
        log(f"Template written to published: '{filename}'")
        return True
    else:
        log(f"Failed to write template '{filename}': HTTP {status}", "ERROR")
        return False


def publish_templates(filenames):
    """No-op: templates are now written directly to live, no push step needed."""
    log(f"Templates written directly to live — no publish step needed")
    return True


# ── Contact ID Extractor ───────────────────────────────────────────
def extract_contact_ids_from_template(template_content):
    """
    Extract contact IDs from template. Supports all formats:
    - crm_object("contact", "ID")  — old live format
    - set c1 = {"firstname": ...}  — new hardcoded dict format (IDs in comment block)
    - contact: 12345               — comment block format
    """
    import re
    ids = set()
    # Old crm_object format
    ids |= set(re.findall(r'crm_object\("contact",\s*"(\d+)"', template_content))
    # Comment block: "contact: 12345678"
    ids |= set(re.findall(r'contact:\s*(\d{8,})', template_content))
    # New comment block: long number followed by spaces and deal ID or status
    # Matches lines like: "  Lisa Diep     22772675  52994241921   amzdealwon [won]"
    ids |= set(re.findall(r'^\s+\S[^\n]+?\s+(\d{8,})\s+(?:\d{8,}|—)', template_content, re.MULTILINE))
    return ids


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
    status_mismatch = bool(live_statuses - template_statuses) or bool(template_statuses - live_statuses - {"NEW"})
    if status_mismatch:
        log(f"  Status changes detected — regenerating template")
        status_changed = True

    # Also trigger if any contact's company name has changed
    # Companies are hardcoded in the template — sync changes from HubSpot
    company_changed = False
    template_companies = set(_re.findall(r'"company":\s*"([^"]*)"\s*[,}]', current_template))
    live_companies = {(c["properties"].get("company") or "").replace('"', '\\"') for c in contacts}
    if live_companies - template_companies or template_companies - live_companies - {""}:
        log(f"  Company name changes detected — regenerating template")
        company_changed = True

    # Also trigger if template still uses old c.company or missing pair[7]
    # Catches cases where Design Manager has old structure — always auto-upgrade
    structure_outdated = (
        "c.company" in current_template or
        "pair[7]" not in current_template or
        "hs_lead_status | lower" in current_template
    )
    if structure_outdated:
        log("  Template structure outdated (missing pair[7] or c.company present) — regenerating")

    # Always check if report_date needs updating — this changes every week
    # regardless of whether contacts/deals changed
    import re as _re2
    date_match = _re2.search(r'set report_date = "([^"]+)"', current_template)
    current_date_in_template = date_match.group(1) if date_match else ""
    expected_date = get_next_monday_date()   # call directly — send_date is scoped to generate_variable_block()
    date_changed  = (current_date_in_template != expected_date)
    if date_changed:
        log(f"  report_date outdated: template has '{current_date_in_template}', expected '{expected_date}'")

    result["changed"] = bool(added or removed or count_changed or status_changed or company_changed or structure_outdated or date_changed)

    if not result["changed"]:
        log("  No changes detected — template is up to date")
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

    log(f"  Template updated successfully: {name}")
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
        slack_notify("Weekly Lead Report Auto-Update FAILED\nHUBSPOT_TOKEN secret is not configured in GitHub.")
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

    mode_note  = " (dry run)" if DRY_RUN else ""
    status_hdr = "ERRORS DETECTED" if any_error else "Complete"

    # Table header
    col_partner = 24
    col_contacts = 9
    col_status = 20
    sep = "-" * (col_partner + col_contacts + col_status + 6)

    lines = [
        f"Weekly Lead Report Auto-Update — {status_hdr}{mode_note}",
        f"{run_date}",
        "",
        f"{'Partner':<{col_partner}}  {'Contacts':<{col_contacts}}  {'Status':<{col_status}}",
        sep,
    ]
    for r in results:
        if r["status"] == "error":
            status_str = "FAILED"
        elif r["status"] == "skipped":
            status_str = "Skipped"
        elif r.get("changed") and r.get("pushed"):
            status_str = "Updated"
        elif not r.get("changed"):
            status_str = "No change"
        else:
            status_str = "Dry run"
        lines.append(
            f"{r['partner']:<{col_partner}}  {str(r['contacts_found']):<{col_contacts}}  {status_str:<{col_status}}"
        )
        if r.get("error"):
            lines.append(f"  Error: {r['error']}")

    lines.append(sep)
    if any_error:
        lines.append("Action required: check Design Manager before Monday 10AM EST.")
    else:
        if not DRY_RUN and files_to_publish:
            lines.append(f"{len(files_to_publish)} template(s) published to Design Manager.")
        lines.append("Monday 10AM EST automated send is ready.")

    slack_notify("```\n" + "\n".join(lines) + "\n```")

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

    log("All partners processed successfully")


if __name__ == "__main__":
    main()
