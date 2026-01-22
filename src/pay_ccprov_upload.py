import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from db.easebase_conn import easebase_conn

def upload_to_postgres(ccprov_csv_path, ccstaff_csv_path, labor_csv_path):
    lab_ledger_df = pd.read_excel(labor_xlsx_path, engine="openpyxl")
    staff_ledger_df = pd.read_csv(ccstaff_csv_path)
    ledger_df = pd.read_csv(ccprov_csv_path)

    conn = easebase_conn()
    cursor = conn.cursor()

    table_name = 'app.clinic_ccprov'
    lab_table_name = 'app.clinic_labor_costs'
    staff_table_name = 'app.clinic_ccstaff'

    batch_size = 100
    rows_to_insert = []
    lab_rows_to_insert = []
    staff_rows_to_insert = []

    cursor.execute('truncate app.clinic_ccprov')
    cursor.execute('truncate app.clinic_ccstaff')
    cursor.execute('truncate app.clinic_labor_costs')

    # ccprov
    for _, row in ledger_df.iterrows():
        rows_to_insert.append(tuple(None if pd.isna(v) else v for v in row))
        if len(rows_to_insert) == batch_size:
            insert_query = f"""INSERT INTO {table_name} (
                ee_id, employee_name, shift_date, day, pay_type, reg_hours, ot1_hours, ot2_hours,
                unpaid_hours, time_in, time_out, cc1, cc2, reg_charge_rate, ot_charge_rate,
                reg_charge_amount, ot_charge_amount, total_charge_amount, reg_pay_rate, ot_pay_rate,
                reg_paid, ot_paid, total_pay_amount
            ) VALUES %s ON CONFLICT DO NOTHING;"""
            execute_values(cursor, insert_query, rows_to_insert)
            conn.commit()
            rows_to_insert = []

    if rows_to_insert:
        insert_query = f"""INSERT INTO {table_name} (
            ee_id, employee_name, shift_date, day, pay_type, reg_hours, ot1_hours, ot2_hours,
            unpaid_hours, time_in, time_out, cc1, cc2, reg_charge_rate, ot_charge_rate,
            reg_charge_amount, ot_charge_amount, total_charge_amount, reg_pay_rate, ot_pay_rate,
            reg_paid, ot_paid, total_pay_amount
        ) VALUES %s ON CONFLICT DO NOTHING;"""
        execute_values(cursor, insert_query, rows_to_insert)
        conn.commit()

    # ccstaff
    for _, row in staff_ledger_df.iterrows():
        staff_rows_to_insert.append(tuple(None if pd.isna(v) else v for v in row))
        if len(staff_rows_to_insert) == batch_size:
            staff_insert_query = f"""INSERT INTO {staff_table_name} (
                ee_id, employee_name, shift_date, day, pay_type, reg_hours, ot1_hours, ot2_hours,
                unpaid_hours, time_in, time_out, cc1, cc2, reg_charge_rate, ot_charge_rate,
                reg_charge_amount, ot_charge_amount, total_charge_amount, reg_pay_rate, ot_pay_rate,
                reg_paid, ot_paid, total_pay_amount
            ) VALUES %s ON CONFLICT DO NOTHING;"""
            execute_values(cursor, staff_insert_query, staff_rows_to_insert)
            conn.commit()
            staff_rows_to_insert = []

    if staff_rows_to_insert:
        staff_insert_query = f"""INSERT INTO {staff_table_name} (
            ee_id, employee_name, shift_date, day, pay_type, reg_hours, ot1_hours, ot2_hours,
            unpaid_hours, time_in, time_out, cc1, cc2, reg_charge_rate, ot_charge_rate,
            reg_charge_amount, ot_charge_amount, total_charge_amount, reg_pay_rate, ot_pay_rate,
            reg_paid, ot_paid, total_pay_amount
        ) VALUES %s ON CONFLICT DO NOTHING;"""
        execute_values(cursor, staff_insert_query, staff_rows_to_insert)
        conn.commit()

    # labor
    for _, row in lab_ledger_df.iterrows():
        lab_rows_to_insert.append(tuple(None if pd.isna(v) else v for v in row))
        if len(lab_rows_to_insert) == batch_size:
            lab_insert_query = f"""INSERT INTO {lab_table_name} (
                company, ee_id, cntlast, cntfirst, cntdept, cndarea, last_check_dt, cc1, cc2,
                reg_hours, reg_amount, ot_hours, ot_amount, bonus_amount, other_amount,
                suta, futa, ss_tax, mcare_tax, other_tax, ret_ben, med_ben, dent_ben, vis_ben, oth_ben
            ) VALUES %s ON CONFLICT DO NOTHING;"""
            execute_values(cursor, lab_insert_query, lab_rows_to_insert)
            conn.commit()
            lab_rows_to_insert = []

    if lab_rows_to_insert:
        lab_insert_query = f"""INSERT INTO {lab_table_name} (
            company, ee_id, cntlast, cntfirst, cntdept, cndarea, last_check_dt, cc1, cc2,
            reg_hours, reg_amount, ot_hours, ot_amount, bonus_amount, other_amount,
            suta, futa, ss_tax, mcare_tax, other_tax, ret_ben, med_ben, dent_ben, vis_ben, oth_ben
        ) VALUES %s ON CONFLICT DO NOTHING;"""
        execute_values(cursor, lab_insert_query, lab_rows_to_insert)
        conn.commit()

    cursor.close()
    conn.close()
