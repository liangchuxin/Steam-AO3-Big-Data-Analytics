import requests
import time
import json
import csv
from typing import Dict, Any, List

BASE_URL = "https://steamspy.com/api.php"
OUTPUT_JSONL = "steamspy_top10000_raw.jsonl"
OUTPUT_CSV = "steamspy_top10000_raw.csv"

START_PAGE = 0
END_PAGE = 9   # 10 pages * 1000 lines/page = 10000 games
SLEEP_SECONDS = 60   #steamspy requirement


def fetch_all_page(page: int, timeout: int = 60, max_retries: int = 3) -> Dict[str, Any]:
    params = {
        "request": "all",
        "page": page
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError(f"Unexpected response format on page {page}: {type(data)}")
            return data
        except Exception as e:
            print(f"[Retry {attempt}/{max_retries}] page={page} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(10)


def normalize_game_record(appid_key: str, record: Dict[str, Any]) -> Dict[str, Any]:
    row = {}

    for k, v in record.items():
        if isinstance(v, (dict, list)):
            row[k] = json.dumps(v, ensure_ascii=False)
        else:
            row[k] = v

    if "appid" not in row or row["appid"] in (None, "", 0):
        try:
            row["appid"] = int(appid_key)
        except Exception:
            row["appid"] = appid_key

    return row


def save_jsonl(records: List[Dict[str, Any]], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def save_csv(records: List[Dict[str, Any]], filename: str) -> None:
    all_fields = set()
    for r in records:
        all_fields.update(r.keys())

    preferred_order = [
        "appid",
        "name",
        "developer",
        "publisher",
        "score_rank",
        "owners",
        "average_forever",
        "average_2weeks",
        "median_forever",
        "median_2weeks",
        "ccu",
        "price",
        "initialprice",
        "discount",
        "tags",
        "languages",
        "genre"
    ]
    remaining = sorted([f for f in all_fields if f not in preferred_order])
    fieldnames = [f for f in preferred_order if f in all_fields] + remaining

    with open(filename, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            writer.writerow(r)


def main():
    all_raw_records = []
    seen_appids = set()

    for page in range(START_PAGE, END_PAGE + 1):
        print(f"Fetching page {page} ...")
        page_data = fetch_all_page(page)

        page_count = 0
        for appid_key, record in page_data.items():
            if not isinstance(record, dict):
                continue

            raw_record = dict(record)
            if "appid" not in raw_record or raw_record["appid"] in (None, "", 0):
                try:
                    raw_record["appid"] = int(appid_key)
                except Exception:
                    raw_record["appid"] = appid_key

            appid_val = raw_record["appid"]
            if appid_val in seen_appids:
                continue
            seen_appids.add(appid_val)

            all_raw_records.append(raw_record)
            page_count += 1

        print(f"Page {page}: collected {page_count} games, total = {len(all_raw_records)}")

        if page < END_PAGE:
            print(f"Sleeping {SLEEP_SECONDS} seconds to respect SteamSpy rate limit for 'all' ...")
            time.sleep(SLEEP_SECONDS)

    save_jsonl(all_raw_records, OUTPUT_JSONL)
    print(f"Saved JSONL to {OUTPUT_JSONL}")

    flat_records = [normalize_game_record(str(r.get("appid", "")), r) for r in all_raw_records]
    save_csv(flat_records, OUTPUT_CSV)
    print(f"Saved CSV to {OUTPUT_CSV}")

    print(f"Total unique games collected: {len(all_raw_records)}")


if __name__ == "__main__":
    main()
