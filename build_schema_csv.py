import polars as pl
from pathlib import Path
import xlrd
import re
import csv


schema_dir = Path("data/schema")
schema_source_filepaths = list(schema_dir.glob("*.xls"))

for filepath in schema_source_filepaths:
    substrings_filename = re.split(r"[_ \.]", str(filepath.name))

    book = xlrd.open_workbook(filepath)
    sheet = book.sheet_by_index(0)

    with open(
        schema_dir / ("_".join(["schema"] + substrings_filename[1:3]) + ".csv"),
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as f:
        writer = csv.writer(f)
        for row_idx in range(2, sheet.nrows):
            writer.writerow(
                [sheet.cell_value(row_idx, col_idx) for col_idx in range(2)]
            )
