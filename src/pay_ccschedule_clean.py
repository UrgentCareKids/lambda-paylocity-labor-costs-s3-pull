import re
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# Shift dates appear in these columns in the uploaded schedules.
DATE_COLS = [4, 6, 8, 11, 14, 15, 17]  # E, G, I, L, O, P, R

NON_SHIFT_VALUES = {
    None,
    '',
    'off',
    'pto',
    'vacation',
    'holiday',
    'n/a',
    'na',
    '-',
}

TIME_RANGE_RE = re.compile(
    r'(?P<start>\d{1,2}(?::\d{2})?\s*(?:AM|PM|A|P)?)\s*[-–]\s*(?P<end>\d{1,2}(?::\d{2})?\s*(?:AM|PM|A|P)?)',
    re.IGNORECASE,
)


def clean_cell(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).replace('\xa0', ' ').strip()
    return text or None


def row_values(df, row_idx):
    return [clean_cell(v) for v in df.iloc[row_idx].tolist()]


def is_clinic_row(values):
    first = values[0] if values else None
    if not first:
        return False
    blocked = {
        'Name', 'Employee', 'Employee ', 'Totals:', 'Manual Attendance',
        'Report by Department', 'Detailed Schedules'
    }
    if first in blocked:
        return False
    return all(v is None for v in values[1:])


def _read_schedule_workbook(path):
    suffix = Path(path).suffix.lower()
    engine = None
    if suffix == '.xlsx':
        engine = 'openpyxl'
    elif suffix == '.xls':
        engine = 'xlrd'

    try:
        return pd.read_excel(path, sheet_name=0, header=None, engine=engine)
    except ImportError as exc:
        if suffix == '.xls':
            raise ImportError(
                "Reading .xls files requires the 'xlrd' package in your Lambda deployment package or layer."
            ) from exc
        raise


def _normalize_ampm(value):
    text = value.strip().upper().replace('.', '')
    if text.endswith('A') and not text.endswith('AM'):
        text += 'M'
    if text.endswith('P') and not text.endswith('PM'):
        text += 'M'
    return text


def _parse_time(value):
    text = _normalize_ampm(value)
    for fmt in ('%I:%M %p', '%I %p', '%H:%M', '%H'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def calculate_shift_hours(shift):
    if shift is None:
        return None

    text = clean_cell(shift)
    if text is None:
        return None
    if text.lower() in NON_SHIFT_VALUES:
        return None

    match = TIME_RANGE_RE.search(text)
    if not match:
        return None

    start = _parse_time(match.group('start'))
    end = _parse_time(match.group('end'))
    if start is None or end is None:
        return None

    if end <= start:
        end += timedelta(days=1)

    hours = round((end - start).total_seconds() / 3600, 2)
    return hours


def parse_workbook(path):
    df = _read_schedule_workbook(path)
    results = []
    current_clinic = None
    current_days = {}
    current_dates = {}
    r = 0

    while r < len(df):
        values = row_values(df, r)
        col_a = values[0]

        if is_clinic_row(values):
            current_clinic = col_a
            r += 1
            continue

        # Header block: previous row has day names, current row starts with Name.
        if col_a == 'Name':
            prev_values = row_values(df, r - 1) if r > 0 else []
            current_days = {
                c: clean_cell(prev_values[c]) if c < len(prev_values) else None
                for c in DATE_COLS
            }
            current_dates = {
                c: clean_cell(values[c]) if c < len(values) else None
                for c in DATE_COLS
            }
            r += 1
            continue

        if col_a is None or col_a == 'Totals:':
            r += 1
            continue

        employee_name = col_a
        employee_number = values[1]
        if employee_number is None or not current_dates:
            r += 1
            continue

        row_plus_1 = row_values(df, r + 1) if r + 1 < len(df) else []
        row_plus_2 = row_values(df, r + 2) if r + 2 < len(df) else []

        for c in DATE_COLS:
            raw_shift = values[c] if c < len(values) else None
            shift_date = current_dates.get(c)
            shift_day = current_days.get(c)
            manual_attendance = raw_shift == 'Manual Attendance'

            if manual_attendance:
                shift = row_plus_2[c] if c < len(row_plus_2) else None
                manual_points = row_plus_1[c] if c < len(row_plus_1) else None
            else:
                shift = raw_shift
                manual_points = None

            shift = clean_cell(shift)
            results.append({
                'cc1': current_clinic,
                'employee_name': employee_name,
                'employee_number': employee_number,
                'shift_date': shift_date,
                'shift_day': shift_day,
                'shift': shift,
                'shift_hours': calculate_shift_hours(shift),
                'manual_attendance': manual_attendance,
                'manual_attendance_points': clean_cell(manual_points),
            })

        r += 1

    final_df = pd.DataFrame(results)
    if final_df.empty:
        return final_df

    parsed_dates = pd.to_datetime(final_df['shift_date'], errors='coerce')
    final_df['shift_date'] = parsed_dates.dt.strftime('%Y-%m-%d').where(
        parsed_dates.notna(),
        final_df['shift_date'],
    )

    ordered_cols = [
        'cc1',
        'employee_name',
        'employee_number',
        'shift_date',
        'shift_day',
        'shift',
        'shift_hours',
        'manual_attendance',
        'manual_attendance_points',
    ]
    return final_df[ordered_cols]


def clean_ccschedule_files(ccschedule_paths, out_dir='/tmp', output_filename='ccschedule_merged_clean.csv'):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    frames = [parse_workbook(path) for path in ccschedule_paths]
    final_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[
        'cc1',
        'employee_name',
        'employee_number',
        'shift_date',
        'shift_day',
        'shift',
        'shift_hours',
        'manual_attendance',
        'manual_attendance_points',
    ])

    csv_path = out_path / output_filename
    final_df.to_csv(csv_path, index=False)
    return str(csv_path), final_df


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Flatten schedule xls/xlsx files into one CSV.')
    parser.add_argument('inputs', nargs='+', help='Input .xls/.xlsx files')
    parser.add_argument('-o', '--output', required=True, help='Output CSV path')
    args = parser.parse_args()

    output_path = Path(args.output)
    csv_path, final_df = clean_ccschedule_files(
        ccschedule_paths=args.inputs,
        out_dir=str(output_path.parent),
        output_filename=output_path.name,
    )
    print(f'Wrote {len(final_df):,} rows to {csv_path}')


if __name__ == '__main__':
    main()
