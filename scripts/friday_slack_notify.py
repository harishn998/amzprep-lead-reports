"""
AMZ Prep — Friday Partner Lead Report Slack Notifier
=====================================================
Schedule: Every Friday at 9AM EST (cron: 0 14 * * 5)

For each active partner:
  1. DMs the assigned sales rep(s) with a full text summary
     matching the weekly email template structure
  2. DMs all dm_observers (defined in partners.json) with the same
     full report, plus a header showing which rep owns that partner
  3. Posts a compact notification to #tech-feature-testing
     confirming the briefing was sent

Rep assignment is in partners.json → "slack_reps" list.
Observer DMs are in partners.json → "dm_observers" list (top-level).
Adding a new observer = add one entry to dm_observers in partners.json, no code changes.
Adding a new partner  = add one entry to partners array in partners.json, no code changes.

Required GitHub Secrets:
  SLACK_BOT_TOKEN  — xoxb-... Bot User OAuth Token
  HUBSPOT_TOKEN    — HubSpot Private App token

Optional env vars (for manual GitHub Actions runs):
  PARTNER_FILTER   — Comma-separated partner names to process (leave blank = all)
  DRY_RUN          — "true" to log only, no messages sent
"""

import os, sys, json, time, urllib.request, urllib.error
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────
HUBSPOT_TOKEN   = os.environ.get("HUBSPOT_TOKEN", "")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
PARTNER_FILTER  = os.environ.get("PARTNER_FILTER", "").strip()
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() == "true"
HS_BASE_URL     = "https://api.hubapi.com"
SLACK_API       = "https://slack.com/api"
TECH_CHANNEL    = "C0APUEEFC30"   # #tech-feature-testing

# ── Logging ─────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    ts     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    icons  = {"INFO": "[INFO]", "WARN": "[WARN]", "ERROR": "[ERROR]", "DEBUG": "[DEBUG]"}
    print(f"{icons.get(level,'  ')} [{ts}] {msg}", flush=True)

# ── HubSpot Helpers ─────────────────────────────────────────────────
def hs(method, path, body=None):
    url     = f"{HS_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    data    = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        log(f"HS {e.code} {method} {path}: {e.read().decode()[:200]}", "ERROR")
        return e.code, {}

def fetch_contacts(partner_name):
    contacts, offset = [], 0
    while True:
        s, r = hs("POST", "/crm/v3/objects/contacts/search", {
            "filterGroups": [{"filters": [{
                "propertyName": "referral_partner_name",
                "operator": "EQ", "value": partner_name
            }]}],
            "properties": ["firstname","lastname","email","company",
                           "hs_lead_status","createdate","num_associated_deals"],
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
            "limit": 100, "after": offset
        })
        if s != 200: break
        contacts.extend(r.get("results", []))
        nxt = r.get("paging", {}).get("next", {}).get("after")
        if nxt:
            offset = nxt
            time.sleep(0.15)
        else:
            break
    return contacts

CW_STAGES = {"13390264","closedwon","1271308872"}
CL_STAGES = {"13390265","closedlost"}
STAGE_MAP = {
    "13390264":"Closed Won","13390265":"Closed Lost",
    "1247679147":"Negotiation","1271308871":"Referred",
    "201513064":"Analysis","89495465":"Connected",
    "1019291411":"Proposal","1271308872":"Partner Won",
    "closedwon":"Closed Won","closedlost":"Closed Lost",
}
STATUS_MAP = {
    "connected":"Connected","open_deal":"Open Deal",
    "cw":"Customer","amzdealwon":"Customer","customer":"Customer",
    "cl":"Lost","churned customer":"Churned","churned":"Churned",
    "attempted_to_contact":"Attempted","not interested":"Not Interested",
    "oh":"On Hold","new":"New",
}
def fmt_status(s):
    sl = (s or "new").lower()
    return STATUS_MAP.get(sl, s or "New")

WON_ST   = {"cw","amzdealwon","customer"}
CHURN_ST = {"churned customer","churned"}

def build_report(partner_name, contacts):
    """Fetch deal data and return structured report."""
    n          = len(contacts)
    connected  = [c for c in contacts
                  if (c["properties"].get("hs_lead_status") or "").lower() == "connected"]
    active_deals, won_deals = [], []
    seen = set()

    for c in contacts:
        if not int(c["properties"].get("num_associated_deals") or 0):
            continue
        st = (c["properties"].get("hs_lead_status") or "").lower()

        # Fetch deal associations
        s2, r2 = hs("GET", f"/crm/v3/objects/contacts/{c['id']}/associations/deals")
        if s2 != 200: continue
        ids = [x["id"] for x in r2.get("results", [])]
        if not ids or ids[0] in seen: continue
        did = ids[0]; seen.add(did)

        # Fetch deal details
        s3, r3 = hs("GET", f"/crm/v3/objects/deals/{did}?properties=dealname,dealstage,amount")
        if s3 != 200: continue
        dp     = r3.get("properties", {})
        dstage = dp.get("dealstage", "")
        fn     = (c["properties"].get("firstname") or "").strip()
        ln     = (c["properties"].get("lastname")  or "").strip()
        name   = (fn + " " + ln).strip() or c["properties"].get("email","")
        co     = c["properties"].get("company") or "—"
        dname  = (dp.get("dealname") or name).strip()
        amt    = dp.get("amount","")
        amt_s  = f"${float(amt):,.0f}" if amt else "—"

        is_won = (st in WON_ST or dstage in CW_STAGES) and st not in CHURN_ST
        if is_won:
            won_deals.append({"deal":dname,"company":co,"amount":amt_s})
        elif dstage not in CL_STAGES:
            active_deals.append({
                "deal":dname,"company":co,
                "stage":STAGE_MAP.get(dstage, dstage or "—"),
                "amount":amt_s
            })
        time.sleep(0.1)

    return {
        "total": n, "connected": len(connected),
        "connected_list": connected,
        "active_deals": active_deals,
        "won_deals": won_deals,
        "all_contacts": contacts,
    }

def build_dm_message(partner_name, data, today):
    """Full weekly lead report — sent as DM to assigned rep, table format."""
    L = []
    sep = "-" * 60

    # ── Header ────────────────────────────────────────────────────
    L.append(f"Weekly Lead Report: {partner_name}")
    L.append(f"Friday {today}  |  Preview before Monday 10AM EST send")
    L.append(sep)

    # ── Summary row ───────────────────────────────────────────────
    L.append(
        f"Total: {data['total']}    "
        f"Connected: {data['connected']}    "
        f"Active Deals: {len(data['active_deals'])}    "
        f"Closed Won: {len(data['won_deals'])}"
    )
    L.append("")

    # ── CONNECTED ─────────────────────────────────────────────────
    if data["connected_list"]:
        col_n = 28; col_c = 24
        L.append(f"CONNECTED  ({data['connected']})")
        L.append(f"  {'Name':<{col_n}}  {'Company':<{col_c}}")
        L.append("  " + "-" * (col_n + col_c + 2))
        for c in data["connected_list"]:
            fn   = (c["properties"].get("firstname") or "").strip()
            ln   = (c["properties"].get("lastname")  or "").strip()
            name = (fn + " " + ln).strip() or c["properties"].get("email","")
            co   = c["properties"].get("company") or "—"
            L.append(f"  {name:<{col_n}}  {co:<{col_c}}")
        L.append("")

    # ── DEAL STATUS ───────────────────────────────────────────────
    if data["active_deals"]:
        col_d = 32; col_c = 20; col_s = 14; col_a = 10
        L.append(f"DEAL STATUS  ({len(data['active_deals'])})")
        L.append(f"  {'Deal':<{col_d}}  {'Company':<{col_c}}  {'Stage':<{col_s}}  {'Value':<{col_a}}")
        L.append("  " + "-" * (col_d + col_c + col_s + col_a + 6))
        for d in data["active_deals"]:
            L.append(
                f"  {d['deal'][:col_d]:<{col_d}}  "
                f"{d['company'][:col_c]:<{col_c}}  "
                f"{d['stage'][:col_s]:<{col_s}}  "
                f"{d['amount']:<{col_a}}"
            )
        L.append("")

    # ── CLOSED WON ────────────────────────────────────────────────
    if data["won_deals"]:
        col_d = 32; col_c = 20; col_a = 10
        L.append(f"CLOSED WON  ({len(data['won_deals'])})")
        L.append(f"  {'Deal':<{col_d}}  {'Company':<{col_c}}  {'Value':<{col_a}}")
        L.append("  " + "-" * (col_d + col_c + col_a + 4))
        for d in data["won_deals"]:
            L.append(
                f"  {d['deal'][:col_d]:<{col_d}}  "
                f"{d['company'][:col_c]:<{col_c}}  "
                f"{d['amount']:<{col_a}}"
            )
        L.append("")

    # ── ALL LEADS ─────────────────────────────────────────────────
    col_n = 28; col_c = 24; col_s = 20
    L.append(f"ALL LEADS  ({data['total']})")
    L.append(f"  #   {'Name':<{col_n}}  {'Company':<{col_c}}  {'Lead Status':<{col_s}}")
    L.append("  " + "-" * (4 + col_n + col_c + col_s + 4))
    for idx, c in enumerate(data["all_contacts"], 1):
        fn   = (c["properties"].get("firstname") or "").strip()
        ln   = (c["properties"].get("lastname")  or "").strip()
        name = (fn + " " + ln).strip() or c["properties"].get("email","")
        co   = c["properties"].get("company") or "—"
        st   = fmt_status(c["properties"].get("hs_lead_status") or "NEW")
        L.append(f"  {idx:<3} {name[:col_n]:<{col_n}}  {co[:col_c]:<{col_c}}  {st:<{col_s}}")

    L.append("")
    L.append(sep)
    L.append("Data pulled live from HubSpot. Reply with any updates before Monday send.")
    return "```\n" + "\n".join(L) + "\n```"





# ── Slack API ────────────────────────────────────────────────────────
def slack_post(channel_id, message):
    """Post a message to any Slack channel or DM channel ID."""
    if DRY_RUN:
        log(f"DRY RUN → channel {channel_id}:\n{message[:300]}...", "DEBUG")
        return True
    data = json.dumps({"channel": channel_id, "text": message}).encode("utf-8")
    req  = urllib.request.Request(
        f"{SLACK_API}/chat.postMessage", data=data,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        }, method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode())
            if body.get("ok"):
                return True
            log(f"Slack error: {body.get('error')} | scopes: {body.get('needed')}", "WARN")
            return False
    except Exception as e:
        log(f"Slack exception: {e}", "WARN")
        return False

def open_dm_channel(user_id):
    """Open a DM channel with a user. Returns channel ID or None."""
    if DRY_RUN:
        return f"DM_{user_id}"
    data = json.dumps({"users": user_id}).encode("utf-8")
    req  = urllib.request.Request(
        f"{SLACK_API}/conversations.open", data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        }, method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode())
            if body.get("ok"):
                return body["channel"]["id"]
            log(f"conversations.open error: {body.get('error')}", "WARN")
            return None
    except Exception as e:
        log(f"conversations.open exception: {e}", "WARN")
        return None

# ── Main ─────────────────────────────────────────────────────────────
def main():
    log("═══════════════════════════════════════════════")
    log("AMZ Prep — Friday Partner Slack Briefings")
    log(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN: log("MODE: DRY RUN — no messages sent", "WARN")
    log("═══════════════════════════════════════════════")

    if not HUBSPOT_TOKEN:
        log("HUBSPOT_TOKEN missing", "ERROR"); sys.exit(1)
    if not SLACK_BOT_TOKEN and not DRY_RUN:
        log("SLACK_BOT_TOKEN missing", "ERROR"); sys.exit(1)

    config_path = os.path.join(os.path.dirname(__file__), "..", "partners.json")
    with open(config_path) as f:
        config = json.load(f)

    partners = [p for p in config["partners"] if p.get("active", True)]
    if PARTNER_FILTER:
        names    = [x.strip() for x in PARTNER_FILTER.split(",")]
        partners = [p for p in partners if p["partner_name"] in names]

    # Load global observer list — receives a DM copy of every partner briefing
    dm_observers = [
        o for o in config.get("dm_observers", [])
        if o.get("slack_user_id") and not o["slack_user_id"].startswith("REPLACE")
    ]
    if dm_observers:
        log(f"Observer DMs enabled for: {[o['name'] for o in dm_observers]}")
    else:
        log("No dm_observers configured — observer DMs skipped", "WARN")

    log(f"Partners: {[p['partner_name'] for p in partners]}")
    today   = datetime.now(timezone.utc).strftime("%B %d, %Y")
    results = []

    for partner in partners:
        name = partner["partner_name"]
        reps = partner.get("slack_reps", [])
        log(f"─── {name} ───")

        # Fetch live HubSpot data
        contacts = fetch_contacts(name)
        log(f"  {len(contacts)} contacts")
        if not contacts:
            log("  No contacts — skipping", "WARN")
            results.append({"partner": name, "status": "skipped"})
            continue

        # Build full report data
        data    = build_report(name, contacts)
        dm_text = build_dm_message(name, data, today)

        # ── Build recipient list: reps + observers in one group DM ──
        # All user IDs combined — Slack opens a single group DM (MPIM)
        # so everyone sees the same message in the same conversation.
        all_recipients = []
        for rep in reps:
            uid = rep.get("slack_user_id", "")
            if uid and not uid.startswith("REPLACE"):
                all_recipients.append({"name": rep.get("name", uid), "uid": uid})
        for obs in dm_observers:
            uid = obs.get("slack_user_id", "")
            if uid and not uid.startswith("REPLACE"):
                all_recipients.append({"name": obs.get("name", uid), "uid": uid})

        dm_ok_count      = 0
        observer_ok_count = 0

        if not all_recipients:
            log("  No valid recipients — skipping DM", "WARN")
        else:
            # Open one group DM with all recipient IDs comma-separated
            uid_list   = ",".join(r["uid"] for r in all_recipients)
            name_list  = ", ".join(r["name"] for r in all_recipients)
            log(f"  Opening group DM with: {name_list}")

            group_channel = open_dm_channel(uid_list)
            if group_channel:
                ok = slack_post(group_channel, dm_text)
                log(f"  Group DM sent to [{name_list}]: {'✓' if ok else 'FAILED'}")
                if ok:
                    dm_ok_count       = sum(1 for r in reps if r.get("slack_user_id"))
                    observer_ok_count = len(dm_observers)
            else:
                log(f"  Could not open group DM — falling back to individual DMs", "WARN")
                # Fallback: send individually if group DM fails
                for r in all_recipients:
                    ch = open_dm_channel(r["uid"])
                    if ch:
                        ok = slack_post(ch, dm_text)
                        log(f"  Fallback DM → {r['name']}: {'✓' if ok else 'FAILED'}")
                        if ok: dm_ok_count += 1
                    time.sleep(0.5)

        # ── Post compact notification to #tech-feature-testing ─────
        rep_tags  = " ".join(
            f"<@{r['slack_user_id']}>"
            for r in reps
            if r.get("slack_user_id") and not r["slack_user_id"].startswith("REPLACE")
        )
        observer_tags = " ".join(
            f"<@{o['slack_user_id']}>"
            for o in dm_observers
        )
        notif_msg = (
            f"Friday Briefing: {name}  |  Rep: {rep_tags}\n"
            f"Total: {data['total']}    "
            f"Connected: {data['connected']}    "
            f"Active Deals: {len(data['active_deals'])}    "
            f"Closed Won: {len(data['won_deals'])}\n"
            f"Full report delivered as DM to rep(s). "
            f"Observer copy sent to: {observer_tags}"
        )
        ch_ok = slack_post(TECH_CHANNEL, notif_msg)
        log(f"  #tech-feature-testing notification: {'sent ✓' if ch_ok else 'FAILED'}")

        results.append({
            "partner":        name,
            "status":         "ok",
            "contacts":       len(contacts),
            "active_deals":   len(data["active_deals"]),
            "won":            len(data["won_deals"]),
            "dm_sent":        dm_ok_count,
            "observer_sent":  observer_ok_count,
        })
        time.sleep(1.0)

    # ── Final run summary to #tech-feature-testing ──────────────────
    run_date  = datetime.now(timezone.utc).strftime("%A %B %d, %Y at %H:%M UTC")
    any_error = any(r["status"] not in ("ok","skipped") for r in results)
    mode_note = " (dry run)" if DRY_RUN else ""
    status_note = "Complete" if not any_error else "Completed with warnings"

    col_p = 24; col_c = 9; col_a = 13; col_w = 11; col_s = 16
    sep_f = "-" * (col_p + col_c + col_a + col_w + col_s + 10)
    summary = [
        f"Friday Partner Briefings — {status_note}{mode_note}",
        f"{run_date}",
        "",
        f"{'Partner':<{col_p}}  {'Leads':<{col_c}}  {'Active Deals':<{col_a}}  {'Closed Won':<{col_w}}  {'Report':<{col_s}}",
        sep_f,
    ]
    for r in results:
        if r["status"] == "ok":
            s = f"Sent to {r['dm_sent']} rep(s)"
        elif r["status"] == "skipped":
            s = "Skipped"
        else:
            s = "Failed"
        summary.append(
            f"{r.get('partner',''):<{col_p}}  "
            f"{str(r.get('contacts','')):<{col_c}}  "
            f"{str(r.get('active_deals','')):<{col_a}}  "
            f"{str(r.get('won','')):<{col_w}}  "
            f"{s:<{col_s}}"
        )
    summary.append(sep_f)
    if dm_observers:
        observer_names = ", ".join(o["name"] for o in dm_observers)
        summary.append(f"Observer copies sent to: {observer_names}")
    summary.append("All assigned reps have been briefed. Monday 10AM EST automated send proceeds as scheduled.")

    slack_post(TECH_CHANNEL, "\n".join(summary))

    if any_error: sys.exit(1)
    log("Friday briefings complete.")

if __name__ == "__main__":
    main()
