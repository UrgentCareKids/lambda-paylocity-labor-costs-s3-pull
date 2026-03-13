import os
import re
from datetime import datetime

import pandas as pd


DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _normalize_text(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _normalize_cc1(val):
    s = _normalize_text(val)
    if not s:
        return None
    return s.upper()


def _excel_date_to_timestamp(val):
    if pd.isna(val):
        return None

    if isinstance(val, pd.Timestamp):
        return val.normalize()

    if isinstance(val, datetime):
        return pd.Timestamp(val.date())

    try:
        parsed = pd.to_datetime(val, errors="coerce")
        if pd.notna(parsed):
            return pd.Timestamp(parsed).normalize()
    except Exception:
        pass

    return None


def _clean_shift_text(val):
    s = _normalize_text(val)
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" - ", "-").replace("- ", "-").replace(" -", "-")
    return s


def _is_time_range(text):
    if not text:
        return False

    text = text.strip().upper().replace("AM", " AM").replace("PM", " PM")
    text = re.sub(r"\s+", " ", text)

    pattern = r"^\d{1,2}:\d{2}\s?(AM|PM)-\d{1,2}:\d{2}\s?(AM|PM)$"
    return bool(re.match(pattern, text))


def _calculate_shift_hours(shift_text):
    if not shift_text:
        return None

    s = shift_text.strip().upper()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("AM", " AM").replace("PM", " PM")
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" - ", "-").replace("- ", "-").replace(" -", "-")

    if not _is_time_range(s):
        return None

    try:
        start_str, end_str = s.split("-", 1)
        start_dt = datetime.strptime(start_str.strip(), "%I:%M %p")
        end_dt = datetime.strptime(end_str.strip(), "%I:%M %p")

        hours = (end_dt - start_dt).total_seconds() / 3600.0
        if hours < 0:
            hours += 24

        return round(hours, 2)
    except Exception:
        return None


def _is_facility_row(cell_a):
    s = _normalize_text(cell_a)
    if not s:
        return False

    s_clean = s.replace(",", "").replace(".", "").replace("-", "").replace("&", "").strip()

    if len(s_clean) < 3:
        return False

    if "," in s:
        return False

    upper_ratio = sum(1 for ch in s_clean if ch.isalpha() and ch.isupper()) / max(
        1, sum(1 for ch in s_clean if ch.isalpha())
    )
    return upper_ratio > 0.8


def _extract_week_dates(row_values):
    dates = []
    for val in row_values:
        dt = _excel_date_to_timestamp(val)
        if dt is not None:
            dates.append(dt)

    if len(dates) >= 7:
        return dates[:7]

    return None


def _looks_like_week_header(row_values):
    text_vals = [str(v).strip().lower() for v in row_values if pd.notna(v)]
    joined = " ".join(text_vals)

    day_hits = sum(day.lower() in joined for day in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
    dates = _extract_week_dates(row_values)

    return day_hits >= 2 and dates is not None and len(dates) == 7


def _parse_employee_row(row):
    cell_a = _normalize_text(row.iloc[0] if len(row) > 0 else None)
    if not cell_a:
        return None

    if "," not in cell_a:
        return None

    employee_name = cell_a
    employee_number = _normalize_text(row.iloc[1] if len(row) > 1 else None)

    return {
        "Employee Name": employee_name,
        "Employee Number": employee_number,
    }


def _safe_cell(row, idx):
    if idx >= len(row):
        return None
    return row.iloc[idx]


def process_ccschedule_file(file_path):
    df = pd.read_excel(file_path, header=None, dtype=object)

    rows_out = []
    current_facility = None
    current_week_dates = None

    i = 0
    while i < len(df):
        row = df.iloc[i]
        cell_a = _normalize_text(_safe_cell(row, 0))

        if _is_facility_row(cell_a):
            current_facility = _normalize_cc1(cell_a)
            i += 1
            continue

        row_vals = row.tolist()
        if _looks_like_week_header(row_vals):
            current_week_dates = _extract_week_dates(row_vals)
            i += 1
            continue

        employee = _parse_employee_row(row)
        if employee and current_facility and current_week_dates:
            manual_row = df.iloc[i + 1] if i + 1 < len(df) else None
            points_row = df.iloc[i + 2] if i + 2 < len(df) else None

            for day_idx in range(7):
                shift_val = _clean_shift_text(_safe_cell(row, day_idx + 2))
                manual_val = _normalize_text(_safe_cell(manual_row, day_idx + 2)) if manual_row is not None else None
                points_val = _normalize_text(_safe_cell(points_row, day_idx + 2)) if points_row is not None else None

                shift_date = current_week_dates[day_idx]
                shift_day = DAY_NAMES[day_idx]

                rows_out.append(
                    {
                        "cc1": current_facility,
                        "Employee Name": employee["Employee Name"],
                        "Employee Number": employee["Employee Number"],
                        "Shift Date": shift_date.strftime("%-m/%-d/%Y") if os.name != "nt" else shift_date.strftime("%#m/%#d/%Y"),
                        "Shift Day": shift_day,
                        "Shift": shift_val,
                        "Shift Hours": _calculate_shift_hours(shift_val),
                        "Manual Attendance": manual_val,
                        "Manual Attendance Points": points_val,
                    }
                )

            i += 3
            continue

        i += 1

    out_df = pd.DataFrame(rows_out)

    if out_df.empty:
        return out_df

    out_df = out_df.drop_duplicates().reset_index(drop=True)
    return out_df


def clean_ccschedule_files(ccschedule_paths, out_dir="/tmp", output_filename="ccschedule_merged_clean.csv"):
    cleaned_frames = []

    for path in ccschedule_paths:
        df = process_ccschedule_file(path)
        if not df.empty:
            df["source_file"] = os.path.basename(path)
            cleaned_frames.append(df)

    if cleaned_frames:
        merged = pd.concat(cleaned_frames, ignore_index=True)
        merged = merged.drop_duplicates(
            subset=[
                "cc1",
                "Employee Name",
                "Employee Number",
                "Shift Date",
                "Shift Day",
                "Shift",
                "Shift Hours",
                "Manual Attendance",
                "Manual Attendance Points",
            ]
        ).reset_index(drop=True)
    else:
        merged = pd.DataFrame(
            columns=[
                "cc1",
                "Employee Name",
                "Employee Number",
                "Shift Date",
                "Shift Day",
                "Shift",
                "Shift Hours",
                "Manual Attendance",
                "Manual Attendance Points",
                "source_file",
            ]
        )

    out_path = os.path.join(out_dir, output_filename)
    merged.to_csv(out_path, index=False)

    return out_path, merged