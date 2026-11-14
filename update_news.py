#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SMCNewsHistory incremental updater (Forex Factory feed -> GitHub repo).

Keeps SMCNewsHistory.json (the dataset consumed by the NinjaTrader 8
indicator SMC-MNQ) up to date with the weekly Forex Factory economic
calendar, fully unattended via GitHub Actions (update_news.yml).

Steps:
1. Reads the existing SMCNewsHistory.json next to this script and preserves
   the whole history (USD High + USD Bank Holiday since 2022-01-01).
2. Downloads ff_calendar_thisweek.json (and ff_calendar_nextweek.json when
   available; a nextweek failure is tolerated, a thisweek failure is fatal).
3. Filters country == "USD" and impact in ("High", "Holiday").
4. Merges by identity (UTC day + normalized title):
     - exact (same day, same title, same UTC time) -> skip
     - same day, different time -> update the stored time (rescheduled)
     - different day, High impact, exactly one future stored row of that
       title within +/-7 days AND exactly one occurrence of that title in
       the feed -> move the stored row (guarded reschedule; protects
       recurring titles such as Unemployment Claims)
     - otherwise -> append as a new row
   forecast/previous are stored empty (dataset convention).
5. Sorts events by (date, title), keeps the ZZZ/Non-Economic meta object
   first and refreshes only its "date" field (last sync timestamp).
6. Writes the file as standard JSON: indent=4, UTF-8 without BOM, LF line
   endings (atomic replace through a .tmp file).

Only the Python standard library is used, so GitHub Actions needs no
pip install. Exit codes: 0 = ok, 1 = fatal error (this-week feed failed,
dataset unreadable, history would shrink, or write failed).
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

THISWEEK_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEXTWEEK_URL = "https://nfs.faireconomy.media/ff_calendar_nextweek.json"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
}

COUNTRY = "USD"
IMPACTS = ("High", "Holiday")
META_COUNTRY = "ZZZ"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CROSS_DAY_WINDOW_DAYS = 7
HTTP_TIMEOUT_SECONDS = 30
HTTP_ATTEMPTS = 3

DEFAULT_META_TITLE = (
    "SMCNewsHistory v2 - ventanas High USD + feriados bancarios USD "
    "desde 2022-01-01 (actualizacion automatica GitHub Actions)"
)


def norm_title(raw):
    """Collapse every whitespace run into a single space and strip ends."""
    if raw is None:
        return ""
    return re.sub(r"\s+", " ", str(raw)).strip()


def to_utc(raw):
    """Feed date (ISO 8601 with explicit ET offset) -> 'yyyy-MM-ddTHH:mm:ssZ'.

    Returns None when the value is missing, naive (no offset), or
    unparseable: such rows are dropped instead of stored with a wrong time."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc).strftime(DATE_FORMAT)


def fetch(url):
    """GET with retries; raises RuntimeError when every attempt fails."""
    last_error = None
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            request = urllib.request.Request(url, headers=HTTP_HEADERS)
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return response.read().decode("utf-8", errors="replace")
        except Exception as exc:  # network layer: log, back off, retry
            last_error = exc
            if attempt < HTTP_ATTEMPTS:
                time.sleep(5 * attempt)
    raise RuntimeError(
        "GET {} failed after {} attempts ({})".format(url, HTTP_ATTEMPTS, last_error)
    )


def load_feed(raw, label, stats):
    """Parse one feed document and return its USD High/Holiday rows in the
    canonical dataset field order."""
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise RuntimeError("{} feed: invalid JSON ({})".format(label, exc))
    if not isinstance(data, list):
        raise RuntimeError("{} feed: unexpected payload (not a list)".format(label))
    stats[label + "_raw"] = len(data)

    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if str(item.get("country", "") or "").strip() != COUNTRY:
            continue
        impact = str(item.get("impact", "") or "").strip()
        if impact not in IMPACTS:
            continue
        title = norm_title(item.get("title"))
        if not title:
            stats["skipped_no_title"] += 1
            continue
        utc = to_utc(item.get("date"))
        if utc is None:
            stats["skipped_bad_date"] += 1
            continue
        out.append({
            "title": title,
            "country": COUNTRY,
            "date": utc,
            "impact": impact,
            "forecast": "",
            "previous": "",
        })
    stats[label + "_filtered"] = len(out)
    return out


def day_ordinal(day_text):
    return datetime.strptime(day_text, "%Y-%m-%d").toordinal()


def cross_day_candidate(events, ev, feed_title_counts, now_str):
    """Return the position of the single stored row this feed event may be a
    reschedule of, or None when the uniqueness guards do not hold."""
    if ev["impact"] != "High":
        return None  # holidays are fixed calendar dates: exact-day matching only
    title = ev["title"]
    if feed_title_counts.get(title, 0) != 1:
        return None  # recurring title within the feed window: never move rows
    target_day = day_ordinal(ev["date"][:10])
    matches = []
    for pos, stored in enumerate(events):
        if str(stored.get("title", "")) != title:
            continue
        stored_date = str(stored.get("date", ""))
        if not stored_date:
            continue
        if stored_date[:10] == ev["date"][:10]:
            continue  # same day: handled by the same-day branch
        if stored_date < now_str:
            continue  # past rows are frozen history, never moved
        if abs(day_ordinal(stored_date[:10]) - target_day) > CROSS_DAY_WINDOW_DAYS:
            continue
        matches.append(pos)
    if len(matches) != 1:
        return None
    return matches[0]


def merge_events(events, feed, now_str, stats):
    """In-place merge of feed rows into the dataset (see module docstring)."""
    index = {}
    for pos, ev in enumerate(events):
        index.setdefault(
            (str(ev.get("date", ""))[:10], str(ev.get("title", ""))), []
        ).append(pos)

    unique_feed = {}
    for ev in feed:
        unique_feed[(ev["date"][:10], ev["title"])] = ev
    deduped_feed = list(unique_feed.values())
    stats["feed_internal_dups"] = len(feed) - len(deduped_feed)

    feed_title_counts = {}
    for ev in deduped_feed:
        feed_title_counts[ev["title"]] = feed_title_counts.get(ev["title"], 0) + 1

    for ev in deduped_feed:
        key = (ev["date"][:10], ev["title"])
        slots = index.get(key)
        if slots:
            stored = events[slots[0]]
            if stored.get("date") == ev["date"]:
                stats["skipped_exact"] += 1
            else:
                stored["date"] = ev["date"]
                stored["forecast"] = ""
                stored["previous"] = ""
                stats["updated_same_day"] += 1
            continue

        pos = cross_day_candidate(events, ev, feed_title_counts, now_str)
        if pos is not None:
            stored = events[pos]
            old_key = (str(stored.get("date", ""))[:10], str(stored.get("title", "")))
            bucket = index.get(old_key)
            if bucket and pos in bucket:
                bucket.remove(pos)
            stored["date"] = ev["date"]
            stored["forecast"] = ""
            stored["previous"] = ""
            index.setdefault(key, []).append(pos)
            stats["moved_cross_day"] += 1
            continue

        events.append({
            "title": ev["title"],
            "country": ev["country"],
            "date": ev["date"],
            "impact": ev["impact"],
            "forecast": "",
            "previous": "",
        })
        index.setdefault(key, []).append(len(events) - 1)
        stats["added"] += 1


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Incremental updater for SMCNewsHistory.json (Forex Factory feed)."
    )
    parser.add_argument(
        "--data",
        default=None,
        help="Path to SMCNewsHistory.json (default: next to this script).",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Write the merged JSON here instead of overwriting --data.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Merge in memory and print the summary without writing.",
    )
    parser.add_argument(
        "--feed-thisweek",
        default=None,
        help="Read the this-week feed from this local file (offline testing).",
    )
    parser.add_argument(
        "--feed-nextweek",
        default=None,
        help="Read the next-week feed from this local file (offline testing).",
    )
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    stats = {
        "skipped_no_title": 0,
        "skipped_bad_date": 0,
        "feed_internal_dups": 0,
        "skipped_exact": 0,
        "updated_same_day": 0,
        "moved_cross_day": 0,
        "added": 0,
    }

    data_path = (
        Path(args.data) if args.data
        else Path(__file__).resolve().parent / "SMCNewsHistory.json"
    )
    if not data_path.is_file():
        print("[FATAL] dataset not found: {}".format(data_path), file=sys.stderr)
        return 1

    try:
        doc = json.loads(data_path.read_bytes().decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        print("[FATAL] cannot read {}: {}".format(data_path, exc), file=sys.stderr)
        return 1
    if not isinstance(doc, list) or not doc:
        print("[FATAL] {}: expected a non-empty JSON array".format(data_path), file=sys.stderr)
        return 1

    meta_rows = []
    events = []
    for obj in doc:
        if not isinstance(obj, dict):
            print("[FATAL] {}: found a non-object entry".format(data_path), file=sys.stderr)
            return 1
        if str(obj.get("country", "")) == META_COUNTRY:
            meta_rows.append(obj)
        else:
            events.append(obj)
    events_before = len(events)

    try:
        if args.feed_thisweek:
            thisweek_raw = Path(args.feed_thisweek).read_text(encoding="utf-8-sig")
        else:
            thisweek_raw = fetch(THISWEEK_URL)
        feed = load_feed(thisweek_raw, "thisweek", stats)
    except Exception as exc:
        print("[FATAL] this-week feed failed: {}".format(exc), file=sys.stderr)
        return 1

    nextweek_raw = None
    if args.feed_nextweek:
        try:
            nextweek_raw = Path(args.feed_nextweek).read_text(encoding="utf-8-sig")
        except OSError as exc:
            print("[WARN] next-week feed file unreadable ({}); continuing".format(exc))
    else:
        try:
            nextweek_raw = fetch(NEXTWEEK_URL)
        except Exception as exc:
            print("[WARN] next-week feed failed ({}); continuing".format(exc))
    if nextweek_raw is not None:
        try:
            feed = feed + load_feed(nextweek_raw, "nextweek", stats)
        except Exception as exc:
            print("[WARN] next-week feed ignored ({}); continuing".format(exc))

    now_str = datetime.now(timezone.utc).strftime(DATE_FORMAT)

    if meta_rows:
        meta = meta_rows[0]
    else:
        meta = {
            "title": DEFAULT_META_TITLE,
            "country": META_COUNTRY,
            "date": "",
            "impact": "Non-Economic",
            "forecast": "",
            "previous": "",
        }
    meta["date"] = now_str

    merge_events(events, feed, now_str, stats)

    if len(events) < events_before:
        print(
            "[FATAL] merge would drop history ({} -> {} rows); aborting without write".format(
                events_before, len(events)
            ),
            file=sys.stderr,
        )
        return 1

    events.sort(key=lambda row: (str(row.get("date", "")), str(row.get("title", ""))))
    out_doc = [meta] + meta_rows[1:] + events
    payload = (json.dumps(out_doc, ensure_ascii=False, indent=4) + "\n").encode("utf-8")

    if args.dry_run:
        print("[DRY-RUN] no file written")
    else:
        out_path = Path(args.out) if args.out else data_path
        tmp_path = out_path.with_name(out_path.name + ".tmp")
        try:
            tmp_path.write_bytes(payload)
            os.replace(str(tmp_path), str(out_path))
        except OSError as exc:
            print("[FATAL] write failed: {}".format(exc), file=sys.stderr)
            return 1

    print(
        "[INFO] dataset: {} events -> {} events (meta date = {})".format(
            events_before, len(events), now_str
        )
    )
    print(
        "[INFO] feed thisweek: raw={} filtered={} | nextweek: raw={} filtered={}".format(
            stats.get("thisweek_raw", 0),
            stats.get("thisweek_filtered", 0),
            stats.get("nextweek_raw", 0),
            stats.get("nextweek_filtered", 0),
        )
    )
    print(
        "[INFO] merge: added={} rescheduled_same_day={} moved_cross_day={} "
        "skipped_exact={} feed_internal_dups={} skipped_bad_date={} skipped_no_title={}".format(
            stats["added"],
            stats["updated_same_day"],
            stats["moved_cross_day"],
            stats["skipped_exact"],
            stats["feed_internal_dups"],
            stats["skipped_bad_date"],
            stats["skipped_no_title"],
        )
    )
    if not args.dry_run:
        out_path = Path(args.out) if args.out else data_path
        print("[INFO] wrote {} bytes to {}".format(len(payload), out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
