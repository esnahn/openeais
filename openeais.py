import io
import re
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile

import dask.dataframe as dd
import pandas as pd
import polars as pl
from dask.diagnostics import ProgressBar
from tqdm import tqdm


def openeais_to_parquet(
    zip_path,
    savedir="data",
    schema_dir="data/schema",
    index=0,
    **kwargs,
) -> Path:
    zip_path = Path(zip_path)
    if zip_path.suffix != ".zip":
        raise ("should be a zip file")

    substrings_filename = re.split(r"[_ +.()]+", zip_path.name)
    results_dir = Path(savedir) / "_".join(substrings_filename[1:5])

    schema_dir = Path(schema_dir)
    schema_filename = "_".join(["schema"] + substrings_filename[1:3]) + ".csv"

    df_schema = pd.read_csv(schema_dir / schema_filename, header=None)
    df_schema[1] = df_schema[1].apply(get_numpy_type)
    df_schema = df_schema.set_index(0)
    schema_dict = df_schema[1].to_dict()

    if results_dir.exists() and results_dir.stat().st_mtime > zip_path.stat().st_mtime:
        print("already done")
        return savedir
    elif results_dir.exists():
        for file in results_dir.iterdir():
            file.unlink()

    try:
        zip_to_parquet(zip_path, results_dir, schema_dict, **kwargs)
    except Exception as e:
        print(f"error on {zip_path}")
        raise (e)

    return results_dir


def zip_to_parquet(
    path,
    results_dir,
    schema_dict,
    member_pattern="*.txt",
    encoding="cp949",
    header=None,
    sep="|",
    index=None,
    **kwargs,
):
    with TemporaryDirectory() as tdir:  # type: str
        with zipfile.ZipFile(path) as zf:
            zf.extractall(path=tdir, members=tqdm(zf.infolist(), desc="Extracting zip"))

        paths = sorted(Path(tdir).glob(member_pattern))
        ddf: dd.DataFrame = dd.read_csv(
            paths,
            encoding=encoding,
            header=header,
            sep=sep,
            names=schema_dict.keys(),
            dtype=schema_dict,
            **kwargs,
        )

        with ProgressBar():
            if index is not None:
                if isinstance(index, int):
                    index = ddf.columns[index]

                print("replacing NA...")
                if schema_dict[index] in ["string", str]:
                    replace_value = ""
                elif schema_dict[index] in ["Int64", "int64", int]:
                    replace_value = 0
                elif schema_dict[index] in ["float64", float]:
                    replace_value = 0.0
                else:
                    raise NotImplementedError(ddf[index].dtype)
                ddf = ddf.fillna({index: replace_value})

                print("Setting index...")
                ddf = ddf.set_index(index)
            print("Saving...")
            ddf.to_parquet(results_dir)


# Oracle Data Types
# https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm

# The CHAR datatype stores fixed-length character strings.

# The VARCHAR2 datatype stores variable-length character strings.
# When you create a table with a VARCHAR2 column, you specify a maximum string
# length (in bytes or characters)

# The NUMBER datatype stores fixed and floating-point numbers.
# The following numbers can be stored in a NUMBER column:
# - Positive numbers in the range 1 x 10-130 to 9.99...9 x 10125 with up to 38 significant digits
# - Negative numbers from -1 x 10-130 to 9.99...99 x 10125 with up to 38 significant digits
# - Zero
# - Positive and negative infinity (generated only by importing from an Oracle Database, Version 5)
# Optionally, you can also specify a precision (total number of digits) and
# scale (number of digits to the right of the decimal point):
# column_name NUMBER (precision, scale)
# Table 26-1 How Scale Factors Affect Numeric Data Storage
# NUMBER(9,2)     7456123.89
# NUMBER(9,1)     7456123.9


# DB types --> dask (pandas/numpy/python) types
# consult https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
# and https://numpy.org/doc/stable/reference/arrays.dtypes.html

# CHAR, VARCHAR --> 'string', str
# NUMERIC(5) --> "Int64" (note the capital "I"; nullable) instead of 'int64', int
# https://pandas.pydata.org/docs/user_guide/integer_na.html
# NUMERIC(19,9) --> 'float64', float (already nullable; try type(np.nan))


def get_polars_type(dtype_openeais: str):
    split_characters = r"[(,)]"

    substrings = re.split(split_characters, dtype_openeais)

    if substrings[0] in ["VARCHAR", "CHAR"]:
        return pl.Utf8
    elif substrings[0] in ["NUMERIC"]:
        if "," in dtype_openeais:
            return pl.Float64
        else:
            return pl.Int64
    else:
        raise


def get_numpy_type(dtype_openeais: str):
    split_characters = r"[(,)]"

    substrings = re.split(split_characters, dtype_openeais)

    if substrings[0] in ["VARCHAR", "CHAR"]:
        return "string"
    elif substrings[0] in ["NUMERIC"]:
        if "," in dtype_openeais:
            return "float64"
        else:
            return "Int64"
    else:
        raise
