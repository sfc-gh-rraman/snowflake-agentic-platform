import openpyxl
import csv
import sys
import os

def export_search_corpus(xlsx_path, csv_path):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb["Search Corpus (Flat)"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in ws.iter_rows(values_only=True):
            writer.writerow(row)

    wb.close()
    row_count = sum(1 for _ in open(csv_path)) - 1
    print(f"Exported {row_count} rows to {csv_path}")

if __name__ == "__main__":
    xlsx = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "references", "dicom_data_model_reference.xlsx"
    )
    csv_out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        os.path.dirname(xlsx), "dicom_model_search_corpus.csv"
    )
    export_search_corpus(xlsx, csv_out)
