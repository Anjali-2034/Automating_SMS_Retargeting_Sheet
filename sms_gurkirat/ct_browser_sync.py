"""
CleverTap Browser Automation → Google Sheets  (SMS / Gurkirat)
--------------------------------------------------------------
Scrapes SMS campaigns created by gurkirat@goodeducator.com and syncs
them to the Gurkirat sheet tab.

Differences from the Shivam script:
  • copies   — full message text from .text-message-preview, everything
               above "Enroll Now" (same selector as the WA scraper).
  • revenue  — Target Group Revenue only  (Shivam sums Target + Control).
  • above baseline — Control Group Revenue only; falls back to Target
               Group Revenue when Control is N/A or 0.

Usage:
    python ct_browser_sync.py --setup          # first-time / session expired
    python ct_browser_sync.py                  # normal daily run
    python ct_browser_sync.py --start 2026-05-01 --end 2026-05-21
"""

import os
import re
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from csv_to_sheets import write_to_sheet

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

CT_LOGIN_EMAIL    = os.getenv("CT_LOGIN_EMAIL")
CT_LOGIN_PASSWORD = os.getenv("CT_LOGIN_PASSWORD")
CT_REGION         = os.getenv("CLEVERTAP_REGION", "eu1")
CT_ACCOUNT_ID     = os.getenv("CLEVERTAP_ACCOUNT_ID")
FILTER_CREATOR    = os.getenv("FILTER_CREATOR", "gurkirat@goodeducator.com")

BASE_URL      = f"https://{CT_REGION}.dashboard.clevertap.com"
CAMPAIGNS_URL = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/sms"

NAV_TIMEOUT  = 30_000
SESSION_FILE = Path(__file__).parent.parent / "ct_session.json"
PROFILE_DIR  = Path(__file__).parent.parent / "ct_browser_profile"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_number(text: str):
    """Strip commas/% and return int, float, or original string.
    Handles negatives including CleverTap's double-dash notation (--27,903 → -27903)."""
    v = text.strip().replace(",", "").replace("%", "").strip()
    if v.startswith("--"):
        v = "-" + v[2:]
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return text.strip()


def _wait_for_text(page, text: str, timeout: int = 20_000):
    try:
        page.wait_for_function(
            f'document.body.innerText.includes({repr(text)})',
            timeout=timeout,
        )
    except PlaywrightTimeout:
        log.warning("Timeout waiting for text: %r", text)


# ── Week date range helpers ────────────────────────────────────────────────────

def current_week_range() -> tuple[date, date]:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def parse_list_date(start_time_raw: str) -> date | None:
    for fmt in ("%b %d, %I:%M %p", "%b %d, %Y, %I:%M %p"):
        try:
            dt = datetime.strptime(start_time_raw.strip(), fmt)
            if dt.year == 1900:
                dt = dt.replace(year=date.today().year)
            return dt.date()
        except ValueError:
            continue
    return None


# ── Channel filter ─────────────────────────────────────────────────────────────

def _apply_channel_filter(page, channel_name: str, creator_email: str | None = None):
    try:
        page.locator(".ct-filter").first.click()
        page.wait_for_function(
            "document.body.innerText.includes('Filter Campaigns')",
            timeout=10_000,
        )
        time.sleep(1.5)

        page.evaluate('''() => {
            const slot = document.querySelector(".ct-selectbox[placeholder='Select channel'] .v-input__slot");
            if (slot) slot.dispatchEvent(new MouseEvent("click", {bubbles: true}));
        }''')
        time.sleep(1.5)
        page.locator(f".v-menu__content .v-list-item:has-text('{channel_name}')").first.click()
        time.sleep(1.5)
        log.info("Channel filter set: %s", channel_name)

        if creator_email:
            page.evaluate('''() => {
                const slot = document.querySelector(".ct-autocomplete[placeholder='Select an email'] .v-input__slot");
                if (slot) slot.dispatchEvent(new MouseEvent("click", {bubbles: true}));
            }''')
            page.wait_for_function(
                f'''() => Array.from(document.querySelectorAll(".v-menu__content .v-list-item"))
                    .some(el => el.innerText.includes("{creator_email}"))''',
                timeout=8_000,
            )
            page.locator(f".v-menu__content .v-list-item:has-text('{creator_email}')").first.click(timeout=5_000)
            time.sleep(1)
            log.info("Creator filter set: %s", creator_email)

        page.locator("button:has-text('Apply')").click()
        time.sleep(3)
        log.info("Filters applied — channel=%s  creator=%s", channel_name, creator_email or "any")
    except Exception as e:
        log.warning("Could not apply filters: %s", e)
        try:
            page.locator("button:has-text('Cancel')").click(timeout=3_000)
            time.sleep(1)
        except Exception:
            try:
                page.keyboard.press("Escape")
                time.sleep(1)
            except Exception:
                pass


# ── Campaign list scraping ─────────────────────────────────────────────────────

def _set_date_filter(page, start: date, end: date):
    try:
        page.locator(".lp-daterangepicker").click()
        page.wait_for_selector(".lp-daterangepicker-dropdown", timeout=10_000)
        time.sleep(1)

        fmt = lambda d: d.strftime("%b %d, %Y")

        inputs = page.locator(".lp-daterangepicker-dropdown input.lp-text-input")
        inputs.nth(0).click(click_count=3)
        inputs.nth(0).fill(fmt(start))
        time.sleep(0.5)
        inputs.nth(1).click(click_count=3)
        inputs.nth(1).fill(fmt(end))
        time.sleep(0.5)

        page.locator(".lp-daterangepicker-dropdown .lp-button.color-primary").click()
        time.sleep(3)
        log.info("Date filter set: %s → %s", fmt(start), fmt(end))
    except Exception as e:
        log.warning("Could not set date filter (%s) — falling back to scroll strategy", e)


def _extract_campaigns_from_page(page) -> list[dict]:
    _scroll_to_load_all(page)
    return page.evaluate(f'''() => {{
        const rows = document.querySelectorAll(".lp-table-row.bordered");
        const results = [];
        for (const row of rows) {{
            if (!row.innerText.includes("Created by: {FILTER_CREATOR}")) continue;
            const linkEl = row.querySelector("a.campaign-details-link");
            if (!linkEl) continue;
            const name = linkEl.innerText.trim();
            const href = linkEl.getAttribute("href") || "";
            const parts = href.split("/");
            const idIdx = parts.indexOf("campaign");
            const campaignId = idIdx >= 0 ? parts[idIdx + 1] : null;
            const cells = row.querySelectorAll(".lp-table-cell");
            const startTime = cells[3] ? cells[3].innerText.trim() : "";
            if (campaignId) results.push({{name, campaignId, startTime}});
        }}
        return results;
    }}''')


def get_campaign_list(page, week_range: tuple[date, date] | None = None) -> list[dict]:
    log.info("Loading SMS campaigns list...")
    page.goto(CAMPAIGNS_URL, wait_until="networkidle", timeout=NAV_TIMEOUT)

    try:
        page.wait_for_selector(".lp-table-row.bordered", timeout=20_000)
    except PlaywrightTimeout:
        log.error("Campaign list did not load. Check session or URL.")
        return []

    time.sleep(2)
    _apply_channel_filter(page, "SMS", creator_email=FILTER_CREATOR)

    if not week_range:
        campaigns = _extract_campaigns_from_page(page)
        log.info("Found %d campaign(s) by %s", len(campaigns), FILTER_CREATOR)
        return campaigns

    start, end = week_range
    all_campaigns: dict[str, dict] = {}

    current = start
    while current <= end:
        _set_date_filter(page, current, current)
        day_campaigns = _extract_campaigns_from_page(page)
        new = [c for c in day_campaigns if c["campaignId"] not in all_campaigns]
        for c in new:
            all_campaigns[c["campaignId"]] = c
        log.info(
            "  %s: %d campaign(s) found (%d new)",
            current.strftime("%d-%m-%Y"), len(day_campaigns), len(new),
        )
        current += timedelta(days=1)

    campaigns = list(all_campaigns.values())
    log.info("Total: %d campaign(s) in range %s – %s", len(campaigns), start, end)
    return campaigns


def _scroll_to_load_all(page):
    container_sel = ".lp-table-body"
    try:
        page.wait_for_selector(container_sel, timeout=10_000)
    except PlaywrightTimeout:
        return

    stable_streak = 0
    prev_count = -1
    for _ in range(50):
        count = page.locator(".lp-table-row.bordered").count()
        if count == prev_count:
            stable_streak += 1
            if stable_streak >= 3:
                break
        else:
            stable_streak = 0
        prev_count = count

        page.evaluate(f'''() => {{
            const el = document.querySelector("{container_sel}");
            if (el) el.scrollTop = el.scrollHeight;
            window.scrollTo(0, document.body.scrollHeight);
        }}''')
        time.sleep(2)


# ── Per-campaign detail scraping ───────────────────────────────────────────────

def scrape_overview(page, campaign_id: str) -> dict:
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/overview"
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    _wait_for_text(page, "ENROLL NOW", timeout=30_000)

    text = page.inner_text("body")

    # Campaign name from page title
    campaign_name = ""
    try:
        title = page.title()
        parts = [p.strip() for p in title.split("|")]
        if len(parts) >= 4:
            campaign_name = parts[-1]
    except Exception:
        pass

    # Date: line after "time\n" — reformat to DD-MM-YYYY
    send_date = ""
    m = re.search(r'\btime\n(.+)', text)
    if m:
        raw_date = m.group(1).strip()
        try:
            dt = datetime.strptime(raw_date, "%b %d, %Y, %I:%M %p")
            send_date = dt.strftime("%d-%m-%Y")
        except ValueError:
            send_date = raw_date

    # Copies: message body text, everything above "ENROLL NOW".
    # SMS overview layout: metadata → ASPECT RATIO / 2:1 → message body → ENROLL NOW
    copies = ""
    # Primary: text between image aspect ratio line and ENROLL NOW
    m = re.search(r'2:1\s*\n+(.*?)\s*\nENROLL NOW', text, re.DOTALL)
    if m:
        copies = m.group(1).strip()
    if not copies:
        # Fallback for text-only campaigns: text after URL redirect line and before ENROLL NOW
        m = re.search(r'URL:.*?\n+(.*?)\s*\nENROLL NOW', text, re.DOTALL)
        if m:
            copies = m.group(1).strip()
    if not copies:
        # Last resort: text between Template Name line and ENROLL NOW
        m = re.search(r'Template Name:.*?\n+(.*?)\s*\nENROLL NOW', text, re.DOTALL)
        if m:
            copies = m.group(1).strip()

    # Control group %
    control_group_pct = ""
    m = re.search(r'Campaign Control Group[^\n]*\n(\d+%)', text)
    if m:
        control_group_pct = m.group(1).strip()

    return {
        "send_date":         send_date,
        "copies":            copies,
        "control_group_pct": control_group_pct,
        "campaign_name":     campaign_name,
    }


def scrape_stats(page, campaign_id: str) -> dict:
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/stats"
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    _wait_for_text(page, "Qualified:", timeout=30_000)
    time.sleep(3)

    text = page.inner_text("body")
    idx = text.find("Qualified:")
    section = text[idx:idx + 2000] if idx >= 0 else text

    qualified = ""
    m = re.search(r'Qualified:\s*([\d,]+)', section)
    if m:
        qualified = _clean_number(m.group(1))

    sent = ""
    m = re.search(r'Sent[^\n]*\n([\d,]+)', section)
    if m:
        sent = _clean_number(m.group(1))

    delivered = ""
    m = re.search(r'Delivered[^\n]*\n[\d.]+%\n([\d,]+)', section)
    if m:
        delivered = _clean_number(m.group(1))

    viewed = ""
    m = re.search(r'Viewed[^\n]*\n[\d.]+%\n([\d,]+)', section)
    if m:
        viewed = _clean_number(m.group(1))

    clicks = ""
    m = re.search(r'Clicks[^\n]*\n[\d.]+%\n([\d,]+)', section)
    if not m:
        m = re.search(r'Clicked[^\n]*\n[\d.]+%\n([\d,]+)', section)
    if m:
        clicks = _clean_number(m.group(1))

    control_group_count = ""
    m = re.search(r'Control group:\s*([\d,]+)', section)
    if m:
        control_group_count = _clean_number(m.group(1))

    return {
        "qualified":           qualified,
        "sent":                sent,
        "delivered":           delivered,
        "viewed":              viewed,
        "clicks":              clicks,
        "control_group_count": control_group_count,
    }


def scrape_conversion(page, campaign_id: str) -> dict:
    """
    revenue        → Target Group Revenue only
    above baseline → Control Group Revenue only;
                     falls back to Target Group Revenue if N/A or 0
    """
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/stats/conversion"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    _wait_for_text(page, "Revenue performance", timeout=30_000)
    time.sleep(3)

    text = page.inner_text("body")
    idx = text.find("Revenue performance")
    section = text[idx:idx + 800] if idx >= 0 else text

    # revenue = Target Group Revenue only
    total_revenue = ""
    m = re.search(r'Target Group Revenue[^\n]*\n([\d,]+(?:\.\d+)?)', section)
    if m:
        total_revenue = _clean_number(m.group(1))

    # above baseline = Control Group Revenue; fallback to Target if N/A or 0
    incremental_revenue = ""
    m = re.search(r'Control Group Revenue[^\n]*\n([\d,]+(?:\.\d+)?)', section)
    if m:
        val = _clean_number(m.group(1))
        if val and val != 0:
            incremental_revenue = val
        else:
            incremental_revenue = total_revenue
    else:
        incremental_revenue = total_revenue

    return {"total_revenue": total_revenue, "incremental_revenue": incremental_revenue}


# ── Main scrape orchestrator ───────────────────────────────────────────────────

def scrape_all_campaigns(page, week_range=None, campaign_ids: list[str] | None = None) -> list[dict]:
    if campaign_ids:
        log.info("Direct-ID mode: scraping %d campaign(s) by ID", len(campaign_ids))
        campaigns = [{"campaignId": cid, "name": None} for cid in campaign_ids]
    else:
        campaigns = get_campaign_list(page, week_range=week_range)
        if not campaigns:
            log.warning("No campaigns found for creator: %s", FILTER_CREATOR)
            return []

    rows = []
    for i, c in enumerate(campaigns, 1):
        name = c["name"]
        cid  = c["campaignId"]
        log.info("[%d/%d] Scraping: %s (ID: %s)", i, len(campaigns), name or "(name from overview)", cid)

        try:
            overview   = scrape_overview(page, cid)
            stats      = scrape_stats(page, cid)
            conversion = scrape_conversion(page, cid)

            if not name:
                name = overview.pop("campaign_name", None) or cid

            campaign_url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{cid}/report/overview"
            row = {"name": name, "campaign_url": campaign_url}
            row.update(overview)
            row.update(stats)
            row.update(conversion)
            rows.append(row)

            log.info(
                "  date=%s  sent=%s  delivered=%s  qualified=%s  ctrl_grp=%s  revenue=%s  incr=%s",
                row.get("send_date"), row.get("sent"), row.get("delivered"),
                row.get("qualified"), row.get("control_group_pct"),
                row.get("total_revenue"), row.get("incremental_revenue"),
            )
        except Exception as e:
            log.error("  Failed to scrape campaign %s: %s", name, e)
            try:
                if "chrome-error://" in page.url or "chromewebdata" in page.url:
                    log.warning("  Browser in error state — recovering...")
                    page.goto("about:blank", wait_until="domcontentloaded", timeout=10_000)
                    time.sleep(1)
            except Exception:
                pass

    log.info("Scraped %d campaign(s) total.", len(rows))
    rows.sort(key=lambda r: r.get("send_date", ""))
    return rows


# ── Setup mode ────────────────────────────────────────────────────────────────

def _setup_with_manual_mfa():
    print("\n" + "="*60)
    print("CLEVERTAP SESSION SETUP")
    print("="*60)

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            accept_downloads=True,
            viewport={"width": 1440, "height": 900},
        )
        page = ctx.new_page()

        print("[1/5] Opening CleverTap login page...")
        page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=NAV_TIMEOUT)
        time.sleep(1)

        print("[2/5] Filling email...")
        page.locator('input[name="username"]').fill(CT_LOGIN_EMAIL)
        page.locator('button[type="submit"]:not(:has-text("Google"))').first.click()
        time.sleep(2)

        print("[3/5] Filling password...")
        pwd = page.locator('input[type="password"]').first
        pwd.wait_for(state="visible", timeout=10_000)
        pwd.fill(CT_LOGIN_PASSWORD)
        page.locator('button[type="submit"]:not(:has-text("Google"))').first.click()
        time.sleep(2)

        print("\n[4/5] Browser window is open and waiting for MFA.")
        print("      → Enter your 6-digit OTP from Google Authenticator")
        print("      → Tick 'Remember this device for 30 days'")
        print("      → Click Continue/Submit")
        print("\n      Waiting automatically — script will proceed once dashboard loads...")

        deadline = time.time() + 180
        last_url = ""
        while time.time() < deadline:
            try:
                current = page.evaluate("window.location.href")
            except Exception:
                current = page.url
            if current != last_url:
                print(f"      [URL] {current}")
                last_url = current
            if CT_ACCOUNT_ID in current:
                break
            if current and "sso.clevertap.com" not in current and "clevertap.com/login" not in current:
                break
            time.sleep(2)
        else:
            raise SystemExit("Login did not complete within 3 minutes. Run --setup again.")

        print(f"\n[5/5] Saving session ...")
        ctx.storage_state(path=str(SESSION_FILE))

        if SESSION_FILE.exists():
            print(f"      Profile saved to: {PROFILE_DIR}/")
            print("\n✓ Setup complete. You can now run: python ct_browser_sync.py")

        ctx.close()


# ── Auto re-login ─────────────────────────────────────────────────────────────

def _auto_relogin(page):
    if not CT_LOGIN_EMAIL or not CT_LOGIN_PASSWORD:
        raise SystemExit("Session expired and CT_LOGIN_EMAIL / CT_LOGIN_PASSWORD are not set.")

    log.info("Session expired — attempting auto re-login...")
    page.goto(f"{BASE_URL}/", wait_until="domcontentloaded", timeout=60_000)
    time.sleep(2)

    page.locator('input[name="username"]').fill(CT_LOGIN_EMAIL)
    page.locator('button[type="submit"]:not(:has-text("Google"))').first.click()
    time.sleep(4)

    try:
        pwd = page.locator('input[type="password"]').first
        pwd.wait_for(state="visible", timeout=20_000)
        pwd.fill(CT_LOGIN_PASSWORD)
        page.locator('button[type="submit"]:not(:has-text("Google"))').first.click()
    except PlaywrightTimeout:
        raise SystemExit("Password field did not appear. Check CT_LOGIN_EMAIL is correct.")

    log.info("Waiting for dashboard after re-login...")
    deadline = time.time() + 60
    while time.time() < deadline:
        current = page.url
        if any(k in current.lower() for k in ("mfa", "otp", "verify", "2fa")):
            raise SystemExit(
                "Re-login hit an MFA step — 30-day device-remember window expired.\n"
                "Run: python ct_browser_sync.py --setup"
            )
        if CT_ACCOUNT_ID and CT_ACCOUNT_ID in current:
            log.info("Auto re-login successful.")
            return
        if "sso.clevertap.com" not in current and "clevertap.com/login" not in current and "clevertap.com" in current:
            log.info("Auto re-login successful — URL: %s", current)
            return
        time.sleep(2)

    raise SystemExit("Auto re-login timed out after 60 s — check credentials.")


# ── Normal sync run ────────────────────────────────────────────────────────────

def run(verify_week: bool = False, start_date: date | None = None, end_date: date | None = None, campaign_ids: list[str] | None = None, dry_run: bool = False):
    if not PROFILE_DIR.exists() and not SESSION_FILE.exists():
        raise SystemExit(
            "No saved session found. Run setup first:\n"
            "    python ct_browser_sync.py --setup"
        )

    week_range = None

    if start_date and end_date:
        week_range = (start_date, end_date)
        log.info("Custom date range: %s to %s", start_date, end_date)
    else:
        is_sunday = date.today().weekday() == 6
        if is_sunday and not verify_week:
            log.info("Today is Sunday — automatically running weekly verification.")
            verify_week = True

        if verify_week:
            monday, sunday = current_week_range()
            week_range = (monday, sunday)
            log.info("Weekly verify mode: %s to %s", monday, sunday)

    with sync_playwright() as pw:
        if PROFILE_DIR.exists():
            ctx = pw.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
                viewport={"width": 1440, "height": 900},
            )
            _browser = None
        else:
            _browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = _browser.new_context(
                storage_state=str(SESSION_FILE),
                viewport={"width": 1440, "height": 900},
            )

        page = ctx.new_page()
        page.set_default_timeout(NAV_TIMEOUT)

        page.goto(CAMPAIGNS_URL, wait_until="domcontentloaded", timeout=120_000)
        if "sso.clevertap.com" in page.url or "clevertap.com/login" in page.url:
            _auto_relogin(page)
            page.goto(CAMPAIGNS_URL, wait_until="domcontentloaded", timeout=120_000)

        rows = scrape_all_campaigns(page, week_range=week_range, campaign_ids=campaign_ids)
        ctx.close()
        if _browser:
            _browser.close()

    if not rows:
        log.warning("No data scraped — nothing written to sheet.")
        return

    if dry_run:
        log.info("=== DRY RUN — would write %d row(s): ===", len(rows))
        for r in rows:
            log.info("  %s", r)
        return

    write_to_sheet(rows)
    log.info("=== Sync complete ===")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CleverTap SMS (Gurkirat) → Google Sheets"
    )
    parser.add_argument("--setup", action="store_true",
                        help="First-time login: opens browser, you handle MFA, session is saved")
    parser.add_argument("--verify-week", action="store_true",
                        help="Re-scrape and verify all campaigns from the current Mon–Sun week")
    parser.add_argument("--start", type=str, default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--end",   type=str, default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--scrape-ids", type=str, default=None, metavar="ID1,ID2,...")
    parser.add_argument("--creator", type=str, default=None, metavar="EMAIL")
    parser.add_argument("--worksheet-gid", type=int, default=None, metavar="GID")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.creator:
        global FILTER_CREATOR
        FILTER_CREATOR = args.creator
    if args.worksheet_gid is not None:
        os.environ["WORKSHEET_GID"] = str(args.worksheet_gid)

    if args.setup:
        _setup_with_manual_mfa()
    else:
        start_date   = date.fromisoformat(args.start) if args.start else None
        end_date     = date.fromisoformat(args.end)   if args.end   else None
        campaign_ids = [i.strip() for i in args.scrape_ids.split(",")] if args.scrape_ids else None
        run(verify_week=args.verify_week, start_date=start_date, end_date=end_date,
            campaign_ids=campaign_ids, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
