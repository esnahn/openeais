from pathlib import Path

from openeais import openeais_to_parquet

for file in Path("data/").glob("국토교통부_*.zip"):
    openeais_to_parquet(file, index=0)
