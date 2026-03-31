"""
AMZ Prep — Friday Partner Lead Report Slack Notifier
=====================================================
Schedule: Every Friday at 9AM EST (cron: 0 14 * * 5)

For each active partner:
  1. DMs the assigned sales rep(s) with a full text summary
     matching the weekly email template structure
  2. Posts a compact notification to #tech-feature-testing
     confirming the briefing was sent

Rep assignment is in partners.json → "slack_reps" list.
Adding a new partner = add one entry to partners.json, no code changes.

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
    icons  = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "DEBUG": "🔍"}
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
    "cw":"Customer ✓","amzdealwon":"Customer ✓","customer":"Customer ✓",
    "cl":"Lost","churned customer":"Churned","churned":"Churned",
    "attempted_to_contact":"Attempted","not interested":"Not Interested",
    "oh":"On Hold","new":"New",
}
STATUS_EMOJI = {
    "connected":"🟢","open_deal":"🔵","cw":"🏆","amzdealwon":"🏆",
    "customer":"🏆","cl":"🔴","churned customer":"⚫","churned":"⚫",
    "attempted_to_contact":"🟡","not interested":"⚫",
    "oh":"⚪","new":"⚪",
}
def fmt_status(s):
    sl = (s or "new").lower()
    return f"{STATUS_EMOJI.get(sl,'⚪')} {STATUS_MAP.get(sl, s or 'New')}"

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
    """Full weekly email template summary — sent as DM to assigned rep."""
    L = []
    L.append(f"*📊 Weekly Lead Report — {partner_name}*")
    L.append(f"_Friday {today} | Your preview before Monday 10AM EST send_")
    L.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Summary cards (matching the email template sections)
    L.append(
        f"*Total Leads:* {data['total']}   "
        f"*Connected:* {data['connected']}   "
        f"*Active Deals:* {len(data['active_deals'])}   "
        f"*Closed Won:* {len(data['won_deals'])}"
    )
    L.append("")

    # ── CONNECTED ──────────────────────────────────────────────────
    if data["connected_list"]:
        L.append(f"*🟢 CONNECTED  ({data['connected']})*")
        for c in data["connected_list"]:
            fn   = (c["properties"].get("firstname") or "").strip()
            ln   = (c["properties"].get("lastname")  or "").strip()
            name = (fn + " " + ln).strip() or c["properties"].get("email","")
            co   = c["properties"].get("company") or "—"
            L.append(f"  • *{name}*  |  _{co}_")
        L.append("")

    # ── DEAL STATUS ────────────────────────────────────────────────
    if data["active_deals"]:
        L.append(f"*🔵 DEAL STATUS  ({len(data['active_deals'])})*")
        for d in data["active_deals"]:
            L.append(f"  • *{d['deal']}*  |  {d['company']}  |  _{d['stage']}_  |  {d['amount']}")
        L.append("")

    # ── CLOSED WON ─────────────────────────────────────────────────
    if data["won_deals"]:
        L.append(f"*🏆 CLOSED WON  ({len(data['won_deals'])})*")
        for d in data["won_deals"]:
            L.append(f"  • *{d['deal']}*  |  {d['company']}  |  {d['amount']}")
        L.append("")

    # ── ALL LEADS ──────────────────────────────────────────────────
    L.append(f"*📋 ALL LEADS  ({data['total']})*")
    for c in data["all_contacts"]:
        fn   = (c["properties"].get("firstname") or "").strip()
        ln   = (c["properties"].get("lastname")  or "").strip()
        name = (fn + " " + ln).strip() or c["properties"].get("email","")
        co   = c["properties"].get("company") or "—"
        st   = (c["properties"].get("hs_lead_status") or "NEW")
        L.append(f"  {fmt_status(st)}  *{name}*  |  _{co}_")

    L.append("")
    L.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    L.append("_Data pulled live from HubSpot. Reply with any changes before Monday's send._")
    return "\n".join(L)

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

        dm_ok_count = 0
        for rep in reps:
            uid      = rep.get("slack_user_id", "")
            rep_name = rep.get("name", uid)

            if not uid or uid.startswith("REPLACE"):
                log(f"  Rep '{rep_name}' has no valid slack_user_id — skipping DM", "WARN")
                continue

            # Open DM channel and send full report
            dm_channel = open_dm_channel(uid)
            if dm_channel:
                ok = slack_post(dm_channel, dm_text)
                log(f"  DM → {rep_name} ({uid}): {'✅ sent' if ok else '❌ FAILED'}")
                if ok: dm_ok_count += 1
            else:
                log(f"  Could not open DM with {rep_name}", "WARN")
            time.sleep(0.5)

        # Post compact notification to #tech-feature-testing
        rep_tags  = " ".join(
            f"<@{r['slack_user_id']}>"
            for r in reps
            if r.get("slack_user_id") and not r["slack_user_id"].startswith("REPLACE")
        )
        notif_msg = (
            f":calendar: *Friday Briefing sent — {name}* | Rep: {rep_tags}\n"
            f"  Total: *{data['total']}*  |  "
            f"Connected: *{data['connected']}*  |  "
            f"Active Deals: *{len(data['active_deals'])}*  |  "
            f"Closed Won: *{len(data['won_deals'])}*\n"
            f"  _Full report sent as DM to rep(s) above._"
        )
        ch_ok = slack_post(TECH_CHANNEL, notif_msg)
        log(f"  #tech-feature-testing: {'✅' if ch_ok else '❌ FAILED'}")

        results.append({
            "partner": name, "status": "ok",
            "contacts": len(contacts),
            "active_deals": len(data["active_deals"]),
            "won": len(data["won_deals"]),
            "dm_sent": dm_ok_count,
        })
        time.sleep(1.0)

    # ── Final run summary to #tech-feature-testing ──────────────────
    run_date  = datetime.now(timezone.utc).strftime("%A %B %d, %Y at %H:%M UTC")
    any_error = any(r["status"] not in ("ok","skipped") for r in results)
    icon      = "✅" if not any_error else "⚠️"
    mode_note = " _(dry run)_" if DRY_RUN else ""

    summary = [
        f"{icon} *All Friday Briefings Complete*{mode_note}",
        f"_{run_date}_\n"
    ]
    for r in results:
        if r["status"] == "ok":
            summary.append(
                f"✅ *{r['partner']}*: {r['contacts']} leads | "
                f"{r['active_deals']} active | {r['won']} won | "
                f"DM sent to {r['dm_sent']} rep(s)"
            )
        elif r["status"] == "skipped":
            summary.append(f"⏭️ *{r['partner']}*: skipped (no contacts)")
        else:
            summary.append(f"❌ *{r['partner']}*: failed")
    summary.append("\n_Reps have been briefed. Monday 10AM EST send proceeds as scheduled._")

    slack_post(TECH_CHANNEL, "\n".join(summary))

    if any_error: sys.exit(1)
    log("All done ✅")

if __name__ == "__main__":
    main()
