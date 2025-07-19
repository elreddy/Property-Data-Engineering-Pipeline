from db import get_connection
from extraction import extract_json

LOOKUPS = {
    "Source": ("source_lookup","source_name"),
    "Selling_Reason": ("selling_reason_lookup","selling_reason"),
    "Final_Reviewer": ("final_reviewer_lookup","reviewer_name"),
}

def load_leads_lookups(file_path):
    data = extract_json(file_path)
    conn = get_connection()
    cursor = conn.cursor()

    for field, (table, column) in LOOKUPS.items():
        unique_values = set()
        for record in data:
            value = record.get(field.capitalize()) or record.get(field)
            if value:
                unique_values.add(value.strip())

        for value in unique_values:
            cursor.execute(f"INSERT IGNORE INTO {table} ({column}) VALUES (%s)", (value,))
        conn.commit()

    cursor.close()
    conn.close()
