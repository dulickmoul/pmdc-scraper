import csv
import sys
import time
from pathlib import Path
import requests

QUAL_URL = "https://hospitals-inspections.pmdc.pk/api/DRC/GetQualifications"
TIMEOUT = 30
SLEEP_SEC = 0.25          # tăng nếu bị chặn
MAX_RETRIES = 5

OUT_CSV = "pmdc_qualifications.csv"   # append vào file chính

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://pmdc.pk",
    "User-Agent": "Mozilla/5.0",
}

def post_quals(session: requests.Session, regno: str) -> dict:
    payload = {"RegistrationNo": regno}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(QUAL_URL, data=payload, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 15))
    raise RuntimeError(f"Failed after retries for {regno}: {last_err}")

def ensure_out_header(path: Path):
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["RegistrationNo", "Degree", "Speciality", "University", "PassingYear"]
        )
        w.writeheader()

def append_rows(path: Path, rows: list[dict]):
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["RegistrationNo", "Degree", "Speciality", "University", "PassingYear"]
        )
        w.writerows(rows)

def main():
    if len(sys.argv) < 2:
        print("Usage: python retry_quals.py <retry_list.csv>")
        sys.exit(1)

    in_path = Path(sys.argv[1])
    out_path = Path(OUT_CSV)
    ensure_out_header(out_path)

    # load registration list
    regnos = []
    with in_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "RegistrationNo" not in reader.fieldnames:
            raise ValueError("Input CSV must have header: RegistrationNo")
        for r in reader:
            x = (r.get("RegistrationNo") or "").strip()
            if x:
                regnos.append(x)

    print(f"Retrying qualifications for {len(regnos)} license(s)...")

    session = requests.Session()

    for i, regno in enumerate(regnos, 1):
        try:
            data = post_quals(session, regno)
            # API format bạn thấy trong devtools: {"status": true, "data": {..., "Qualifications":[...]}, "message": ...}
            block = data.get("data") if isinstance(data, dict) else None
            quals = []
            if isinstance(block, dict):
                qlist = block.get("Qualifications") or block.get("Qualifications".lower())  # just in case
                if isinstance(qlist, list):
                    for q in qlist:
                        if not isinstance(q, dict):
                            continue
                        quals.append({
                            "RegistrationNo": regno,
                            "Degree": (q.get("Degree") or "").strip(),
                            "Speciality": (q.get("Speciality") or "").strip(),
                            "University": (q.get("University") or "").strip(),
                            "PassingYear": (q.get("PassingYear") or "").strip(),
                        })

            # Nếu không có qualification, vẫn log 1 dòng trống để bạn biết đã retry
            if not quals:
                quals = [{
                    "RegistrationNo": regno,
                    "Degree": "",
                    "Speciality": "",
                    "University": "",
                    "PassingYear": "",
                }]

            append_rows(out_path, quals)
            print(f"[{i}/{len(regnos)}] OK {regno} (+{len(quals)} row)")
            time.sleep(SLEEP_SEC)

        except Exception as e:
            print(f"[{i}/{len(regnos)}] ERROR {regno}: {e}")

    print("Done.")

if __name__ == "__main__":
    main()
