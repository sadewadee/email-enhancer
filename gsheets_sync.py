import os
import csv
import time
from typing import Optional, List

import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def _get_client() -> gspread.Client:
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "euphoric-effect-320216-a7d3a6461155.json")
    if not cred_path or not os.path.exists(cred_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS tidak diset atau file tidak ditemukan")
    creds = Credentials.from_service_account_file(cred_path, scopes=SCOPES)
    return gspread.authorize(creds)


def _ensure_spreadsheet(client: gspread.Client, spreadsheet_id: Optional[str]) -> gspread.Spreadsheet:
    if spreadsheet_id:
        return client.open_by_key(spreadsheet_id)
    name = f"Email Enhancer {time.strftime('%Y-%m-%d %H:%M:%S')}"
    return client.create(name)


def _sanitize(value):
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return value


def _batch_update(worksheet: gspread.Worksheet, rows: List[List]):
    if not rows:
        return
    max_chunk = 5000
    start_row = 1
    for i in range(0, len(rows), max_chunk):
        chunk = rows[i : i + max_chunk]
        end_row = start_row + len(chunk) - 1
        end_col = len(chunk[0]) if chunk else 1
        rng = gspread.utils.rowcol_to_a1(start_row, 1) + ":" + gspread.utils.rowcol_to_a1(end_row, end_col)
        retries = 0
        delay = 1.0
        while True:
            try:
                worksheet.update(rng, chunk, value_input_option="USER_ENTERED")
                break
            except Exception as e:
                msg = str(e).lower()
                if ("429" in msg) or ("rate" in msg) or ("quota" in msg):
                    time.sleep(delay)
                    retries += 1
                    delay = 2 * delay if delay < 16 else 16
                    if retries >= 5:
                        raise
                else:
                    raise
        time.sleep(0.2)
        start_row = end_row + 1


def _split_emails(raw: str) -> List[str]:
    if not raw:
        return []
    tokens = [p.strip() for p in raw.split(';') if p.strip()]
    cleaned = [t for t in tokens if t.lower() not in {"nan", "none", "null", "-"}]
    return cleaned[:4]


def sync_csv_to_sheet(csv_path: str, spreadsheet_id: Optional[str] = None, sheet_name: Optional[str] = None, replace: bool = True) -> str:
    client = _get_client()
    ss = _ensure_spreadsheet(client, spreadsheet_id)
    title = sheet_name or os.path.splitext(os.path.basename(csv_path))[0]
    try:
        ws = ss.worksheet(title)
        if replace:
            ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=1, cols=1)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return ss.id

    header = rows[0]
    data = rows[1:]
    idx = {k: i for i, k in enumerate(header)}

    # Mandatory columns that must match csv_processor.py output
    target = [
        "No",
        "name",
        "street",
        "city",
        "country_code",
        "url",
        "phone_number",
        "google_business_categories",
        "facebook",
        "instagram",
        "emails",  # Scraped emails (will be split into emails, emails_2, emails_3, emails_4)
        "phones",  # Scraped phones
        "whatsapp",  # Scraped whatsapp
        "email",  # Original email column from input CSV (moved to after whatsapp)
    ]

    need_e2 = False
    need_e3 = False
    need_e4 = False

    parsed_emails: List[List[str]] = []
    for row in data:
        raw_emails = row[idx["emails"]] if "emails" in idx and idx["emails"] < len(row) else ""
        parts = _split_emails(raw_emails)
        parsed_emails.append(parts)
        if len(parts) > 1:
            need_e2 = True
        if len(parts) > 2:
            need_e3 = True
        if len(parts) > 3:
            need_e4 = True

    out_header = target[:]
    if need_e2:
        out_header.append("emails_2")
    if need_e3:
        out_header.append("emails_3")
    if need_e4:
        out_header.append("emails_4")

    def _is_placeholder(s: str) -> bool:
        return (s or "").strip().lower() in {"nan", "none", "null", "-"}

    def _clean(s: str) -> str:
        if s is None:
            return ""
        s2 = str(s).strip()
        return "" if _is_placeholder(s2) else s2

    out_rows: List[List] = [out_header]
    for i, row in enumerate(data):
        def get(k: str) -> str:
            return row[idx[k]] if k in idx and idx[k] < len(row) else ""

        base = [
            get("No"),
            get("name"),
            get("street"),
            get("city"),
            get("country_code"),
            get("url"),
            get("phone_number"),
            get("google_business_categories"),
            get("facebook"),
            get("instagram"),
            parsed_emails[i][0] if parsed_emails[i] else "",  # Scraped emails (first email)
            get("phones"),  # Scraped phones
            get("whatsapp"),  # Scraped whatsapp
            get("email"),  # Original email column from input CSV (moved to after whatsapp)
        ]

        if need_e2:
            base.append(parsed_emails[i][1] if len(parsed_emails[i]) > 1 else "")
        if need_e3:
            base.append(parsed_emails[i][2] if len(parsed_emails[i]) > 2 else "")
        if need_e4:
            base.append(parsed_emails[i][3] if len(parsed_emails[i]) > 3 else "")

        out_rows.append([_sanitize(_clean(v)) for v in base])

    rows_needed = len(out_rows)
    cols_needed = max(len(r) for r in out_rows) if out_rows else 1
    ws.resize(rows_needed, cols_needed)
    _batch_update(ws, out_rows)
    return ss.id


 


def build_global_summary(spreadsheet_id: str, summary_sheet: str = "Summary"):
    client = _get_client()
    ss = client.open_by_key(spreadsheet_id)
    worksheets = [w for w in ss.worksheets() if w.title != summary_sheet]

    def split_list(raw: str) -> List[str]:
        if not raw:
            return []
        return [p.strip() for p in raw.split(';') if p.strip()]

    out_rows: List[List] = [[
        "Sheet Name",
        "Total Rows",
        "Unique Countries",
        "Phone Count",
        "Email Count",
        "URL Count",
    ]]

    totals = {
        "rows": 0,
        "unique_countries": 0,
        "phone_count": 0,
        "email_count": 0,
        "url_count": 0,
    }

    for w in worksheets:
        values = w.get_all_values()
        if not values:
            continue
        header = values[0]
        data = values[1:]
        idx = {k: i for i, k in enumerate(header)}

        total_rows = len(data)

        cc_idx = idx.get("country_code", -1)
        unique_countries = 0
        if cc_idx >= 0:
            unique_countries = len({row[cc_idx].strip() for row in data if cc_idx < len(row) and row[cc_idx].strip()})

        phone_idx = idx.get("phone_number", -1)
        phone_count = 0
        if phone_idx >= 0:
            for row in data:
                phone_count += len(split_list(row[phone_idx] if phone_idx < len(row) else ""))

        email_cols = [c for c in ("emails", "emails_2", "emails_3", "emails_4") if c in idx]
        email_count = 0
        if email_cols:
            for row in data:
                for c in email_cols:
                    ci = idx[c]
                    if ci < len(row) and row[ci].strip() and row[ci].strip().lower() not in {"nan", "none", "null", "-"}:
                        email_count += 1

        url_idx = idx.get("url", -1)
        url_count = 0
        if url_idx >= 0:
            for row in data:
                if url_idx < len(row) and row[url_idx].strip():
                    url_count += 1

        out_rows.append([
            w.title,
            total_rows,
            unique_countries,
            phone_count,
            email_count,
            url_count,
        ])

        totals["rows"] += total_rows
        totals["unique_countries"] += unique_countries
        totals["phone_count"] += phone_count
        totals["email_count"] += email_count
        totals["url_count"] += url_count

    out_rows.append([
        "TOTAL",
        totals["rows"],
        totals["unique_countries"],
        totals["phone_count"],
        totals["email_count"],
        totals["url_count"],
    ])

    try:
        ws = ss.worksheet(summary_sheet)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=summary_sheet, rows=1, cols=1)
    ws.resize(len(out_rows), len(out_rows[0]) if out_rows else 1)
    _batch_update(ws, out_rows)
