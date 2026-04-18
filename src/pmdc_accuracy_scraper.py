import csv
import json
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

GETDATA_URL = "https://hospitals-inspections.pmdc.pk/api/DRC/GetData"
GETQUAL_URL = "https://hospitals-inspections.pmdc.pk/api/DRC/GetQualifications"

DB_PATH = "seen.sqlite"
STATE_PATH = "state.json"
PREFIX_AUDIT_CSV = "prefix_audit.csv"

OUT_LICENSES = "pmdc_licenses.csv"
OUT_QUALS = "pmdc_qualifications.csv"

# Accuracy-first tuning
PAGE_SIZE = 200          # safer (less stress, less timeouts). You can raise to 500-1000 later.
TIMEOUT = 60
MIN_SLEEP = 0.25
JITTER = 0.35
MAX_RETRIES = 7
BACKOFF_BASE = 1.7

# Prefix splitting control
MAX_DEPTH = 3            # A..Z then AA..ZZ then AAA..ZZZ (usually enough)
SPLIT_THRESHOLD = 20000  # if a prefix returns more than this, split deeper

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://pmdc.pk",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
}

ALPHABET = [chr(c) for c in range(ord("A"), ord("Z") + 1)]


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sleep_polite(mult: float = 1.0) -> None:
    time.sleep((MIN_SLEEP + random.random() * JITTER) * mult)


def robust_post(session: requests.Session, url: str, data: Dict[str, Any]) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, data=data, headers=HEADERS, timeout=TIMEOUT)
            # PMDC generally returns JSON
            return resp.json()
        except Exception as e:
            last_err = e
            backoff = (BACKOFF_BASE ** (attempt - 1))
            sleep_polite(mult=backoff)
    raise RuntimeError(f"POST failed after {MAX_RETRIES} retries: {last_err}")


def init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS registrations (
            reg TEXT PRIMARY KEY,
            qual_done INTEGER DEFAULT 0
        )
    """)
    con.commit()
    con.close()


def db_add_regs(regs: List[str]) -> int:
    if not regs:
        return 0
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    inserted = 0
    for r in regs:
        try:
            cur.execute("INSERT OR IGNORE INTO registrations(reg, qual_done) VALUES (?, 0)", (r,))
            if cur.rowcount == 1:
                inserted += 1
        except Exception:
            pass
    con.commit()
    con.close()
    return inserted


def db_next_regs(limit: int = 100) -> List[str]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT reg FROM registrations WHERE qual_done=0 ORDER BY reg LIMIT ?", (limit,))
    rows = [r[0] for r in cur.fetchall()]
    con.close()
    return rows


def db_mark_qual_done(reg: str) -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE registrations SET qual_done=1 WHERE reg=?", (reg,))
    con.commit()
    con.close()


def load_state() -> Dict[str, Any]:
    # If state is corrupted / old schema, we reset safely
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                st = json.load(f)
            if not isinstance(st, dict):
                raise ValueError("bad state")
            # ensure keys
            st.setdefault("phase", "enumerate_prefixes")
            st.setdefault("queue", [])
            st.setdefault("queue_idx", 0)
            st.setdefault("fetched_at", None)
            return st
        except Exception:
            pass

    return {
        "phase": "enumerate_prefixes",
        "queue": ALPHABET[:],   # A..Z
        "queue_idx": 0,
        "fetched_at": utc_iso(),
    }


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


def ensure_csv(path: str, headers: List[str]) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)


def append_rows_csv(path: str, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_csv(path, headers)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        for r in rows:
            w.writerow({k: r.get(k, "") for k in headers})


def parse_total(resp: Dict[str, Any]) -> Optional[int]:
    # Sometimes message = "34318 Records Found!"
    msg = resp.get("message")
    if isinstance(msg, str):
        m = re.search(r"(\d+)\s+Records\s+Found", msg, flags=re.I)
        if m:
            return int(m.group(1))
        m2 = re.search(r"(\d+)", msg)
        if m2:
            return int(m2.group(1))
    # DataTables shape
    if "recordsTotal" in resp and isinstance(resp.get("recordsTotal"), (int, float, str)):
        try:
            return int(resp["recordsTotal"])
        except Exception:
            return None
    return None


def extract_rows(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = resp.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def getdata_payload(prefix: str, start: int, length: int) -> Dict[str, Any]:
    # We search using Name only. RegistrationNo & FatherName blank.
    # Keep start/length because paging is used by the site.
    return {
        "RegistrationNo": "",
        "Name": prefix,
        "FatherName": "",
        "start": str(start),
        "length": str(length),
    }


def enumerate_prefix(session: requests.Session, prefix: str, depth: int) -> Tuple[int, int]:
    """
    Returns (total_reported, collected_regs_count_for_this_prefix)
    """
    # First call: determine total
    resp0 = robust_post(session, GETDATA_URL, getdata_payload(prefix, 0, PAGE_SIZE))
    total = parse_total(resp0) or 0
    rows0 = extract_rows(resp0)

    regs = []
    for r in rows0:
        reg = (r.get("RegistrationNo") or "").strip()
        if reg:
            regs.append(reg)

    # If total is huge, we don't paginate; we split prefix
    if total > SPLIT_THRESHOLD and depth < MAX_DEPTH:
        return total, -1  # signal "needs split"

    # Otherwise, paginate through all pages
    collected = len(regs)
    start = PAGE_SIZE
    while True:
        sleep_polite()
        resp = robust_post(session, GETDATA_URL, getdata_payload(prefix, start, PAGE_SIZE))
        rows = extract_rows(resp)
        if not rows:
            break
        batch = []
        for r in rows:
            reg = (r.get("RegistrationNo") or "").strip()
            if reg:
                batch.append(reg)
        regs.extend(batch)
        collected += len(batch)
        start += PAGE_SIZE

        # Safety: if total known and we already got >= total, stop
        if total and collected >= total:
            break

    inserted = db_add_regs(list(dict.fromkeys(regs)))
    return total, inserted


def fetch_qualifications(session: requests.Session, reg: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    resp = robust_post(session, GETQUAL_URL, {"RegistrationNo": reg})
    data = resp.get("data")

    doctor: Dict[str, Any] = {}
    quals: List[Dict[str, Any]] = []

    if isinstance(data, dict):
        doctor = data.copy()
        q = data.get("Qualifications")
        if isinstance(q, list):
            for item in q:
                if isinstance(item, dict):
                    quals.append(item)
        else:
            # sometimes null
            quals = []
    else:
        doctor = {"RegistrationNo": reg}

    doctor["RegistrationNo"] = reg
    return doctor, quals


def main() -> None:
    init_db()
    state = load_state()

    ensure_csv(PREFIX_AUDIT_CSV, ["prefix", "depth", "reported", "inserted_new_regs", "status", "ts"])
    ensure_csv(OUT_LICENSES, [
        "RegistrationNo", "Name", "FatherName", "Gender", "RegistrationType",
        "RegistrationDate", "ValidUpto", "Status", "IsFaculty"
    ])
    ensure_csv(OUT_QUALS, ["RegistrationNo", "Degree", "Speciality", "University", "PassingYear", "IsActive"])

    with requests.Session() as session:
        # Phase 1: enumerate prefixes into RegistrationNo universe
        if state["phase"] == "enumerate_prefixes":
            queue: List[str] = state.get("queue", ALPHABET[:])
            idx = int(state.get("queue_idx", 0))

            while idx < len(queue):
                prefix = queue[idx]
                depth = len(prefix)
                print(f"[ENUM] prefix={prefix} depth={depth} ({idx+1}/{len(queue)})")

                try:
                    reported, inserted = enumerate_prefix(session, prefix, depth)
                    if inserted == -1:
                        # Split into deeper prefixes
                        new_prefixes = [prefix + ch for ch in ALPHABET]
                        queue.extend(new_prefixes)
                        status = "SPLIT"
                        ins = ""
                    else:
                        status = "OK"
                        ins = str(inserted)

                    # audit log
                    with open(PREFIX_AUDIT_CSV, "a", newline="", encoding="utf-8") as f:
                        w = csv.writer(f)
                        w.writerow([prefix, depth, reported, ins, status, utc_iso()])

                except Exception as e:
                    with open(PREFIX_AUDIT_CSV, "a", newline="", encoding="utf-8") as f:
                        w = csv.writer(f)
                        w.writerow([prefix, depth, "", "", f"ERROR:{e}", utc_iso()])
                    print(f"[ENUM][ERROR] {prefix}: {e}")
                    sleep_polite(mult=2.0)

                idx += 1
                state["queue"] = queue
                state["queue_idx"] = idx
                save_state(state)

            # Done enumeration
            state["phase"] = "fetch_qualifications"
            save_state(state)
            print("[ENUM] Done. Moving to qualification phase.")

        # Phase 2: fetch qualifications + doctor details for all regs in DB
        if state["phase"] == "fetch_qualifications":
            while True:
                regs = db_next_regs(limit=50)
                if not regs:
                    break

                for reg in regs:
                    print(f"[QUAL] {reg}")
                    try:
                        doctor, quals = fetch_qualifications(session, reg)

                        # Write doctor row (doctor includes Qualifications array too, ignore it here)
                        lic_row = {
                            "RegistrationNo": doctor.get("RegistrationNo", ""),
                            "Name": doctor.get("Name", ""),
                            "FatherName": doctor.get("FatherName", ""),
                            "Gender": doctor.get("Gender", ""),
                            "RegistrationType": doctor.get("RegistrationType", ""),
                            "RegistrationDate": doctor.get("RegistrationDate", ""),
                            "ValidUpto": doctor.get("ValidUpto", ""),
                            "Status": doctor.get("Status", ""),
                            "IsFaculty": doctor.get("IsFaculty", ""),
                        }
                        append_rows_csv(OUT_LICENSES, [
                            "RegistrationNo", "Name", "FatherName", "Gender", "RegistrationType",
                            "RegistrationDate", "ValidUpto", "Status", "IsFaculty"
                        ], [lic_row])

                        # Write qualification rows (one row per qualification)
                        qual_rows = []
                        if quals:
                            for q in quals:
                                qual_rows.append({
                                    "RegistrationNo": reg,
                                    "Degree": (q.get("Degree") or ""),
                                    "Speciality": (q.get("Speciality") or ""),
                                    "University": (q.get("University") or ""),
                                    "PassingYear": (q.get("PassingYear") or ""),
                                    "IsActive": q.get("IsActive", ""),
                                })
                        else:
                            qual_rows.append({
                                "RegistrationNo": reg,
                                "Degree": "",
                                "Speciality": "",
                                "University": "",
                                "PassingYear": "",
                                "IsActive": "",
                            })

                        append_rows_csv(OUT_QUALS,
                                        ["RegistrationNo", "Degree", "Speciality", "University", "PassingYear", "IsActive"],
                                        qual_rows)

                        db_mark_qual_done(reg)
                        sleep_polite()

                    except Exception as e:
                        print(f"[QUAL][ERROR] {reg}: {e}")
                        sleep_polite(mult=2.0)

            state["phase"] = "done"
            save_state(state)
            print("[DONE] Completed. Files:")
            print(f" - {OUT_LICENSES}")
            print(f" - {OUT_QUALS}")
            print(f" - {PREFIX_AUDIT_CSV}")
            print(f" - {DB_PATH}")


if __name__ == "__main__":
    main()
