# hoa_load_lookups.py

from db import get_connection
from extraction import extract_json

def load_hoa_lookup(file_path):
    data = extract_json(file_path)
    conn = get_connection()
    cursor = conn.cursor()

    unique_hoa_records = set()

    for row in data:
        hoa_list = row.get("HOA", [])
        for hoa_entry in hoa_list:
            hoa_value = hoa_entry.get("HOA")
            hoa_flag = hoa_entry.get("HOA_Flag")
            if hoa_value is not None and hoa_flag is not None:
                unique_hoa_records.add((hoa_value, hoa_flag))

    for value, flag in unique_hoa_records:
        cursor.execute(
            "INSERT IGNORE INTO hoa_lookup (hoa_value, hoa_flag) VALUES (%s, %s)",
            (value, flag)
        )

    conn.commit()
    cursor.close()
    conn.close()

