from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Mapping
import zipfile
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from tqdm import tqdm


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


# 건축물대장 기본개요

bldrgst_dtypes = {
    "관리_건축물대장_PK": str,
    "관리_상위_건축물대장_PK": str,
    "대장_구분_코드": str,
    "대장_구분_코드_명": str,
    "대장_종류_코드": str,
    "대장_종류_코드_명": str,
    "대지_위치": str,
    "도로명_대지_위치": str,
    "건물_명": str,
    "시군구_코드": str,
    "법정동_코드": str,
    "대지_구분_코드": str,
    "번": str,
    "지": str,
    "특수지_명": str,
    "블록": str,
    "로트": str,
    "외필지_수": "Int64",
    "새주소_도로_코드": str,
    "새주소_법정동_코드": str,
    "새주소_지상지하_코드": str,
    "새주소_본_번": "Int64",
    "새주소_부_번": "Int64",
    "지역_코드": str,
    "지구_코드": str,
    "구역_코드": str,
    "지역_코드_명": str,
    "지구_코드_명": str,
    "구역_코드_명": str,
    "생성_일자": str,
}

# 건축물대장 총괄표제부 recapitulation

recap_dtypes = {
    "관리_건축물대장_PK": str,
    "대장_구분_코드": str,
    "대장_구분_코드_명": str,
    "대장_종류_코드": str,
    "대장_종류_코드_명": str,
    "신_구_대장_구분_코드": str,
    "신_구_대장_구분_코드_명": str,
    "대지_위치": str,
    "도로명_대지_위치": str,
    "건물_명": str,
    "시군구_코드": str,
    "법정동_코드": str,
    "대지_구분_코드": str,
    "번": str,
    "지": str,
    "특수지_명": str,
    "블록": str,
    "로트": str,
    "외필지_수": "Int64",
    "새주소_도로_코드": str,
    "새주소_법정동_코드": str,
    "새주소_지상지하_코드": str,
    "새주소_본_번": "Int64",
    "새주소_부_번": "Int64",
    "대지_면적(㎡)": float,
    "건축_면적(㎡)": float,
    "건폐_율(%)": float,
    "연면적(㎡)": float,
    "용적_률_산정_연면적(㎡)": float,
    "용적_률(%)": float,
    "주_용도_코드": str,
    "주_용도_코드_명": str,
    "기타_용도": str,
    "세대_수(세대)": "Int64",
    "가구_수(가구)": "Int64",
    "주_건축물_수": "Int64",
    "부속_건축물_수": "Int64",
    "부속_건축물_면적(㎡)": float,
    "총_주차_수": "Int64",
    "옥내_기계식_대수(대)": "Int64",
    "옥내_기계식_면적(㎡)": float,
    "옥외_기계식_대수(대)": "Int64",
    "옥외_기계식_면적(㎡)": float,
    "옥내_자주식_대수(대)": "Int64",
    "옥내_자주식_면적(㎡)": float,
    "옥외_자주식_대수(대)": "Int64",
    "옥외_자주식_면적(㎡)": float,
    "허가_일": str,
    "착공_일": str,
    "사용승인_일": str,
    "허가번호_년": str,
    "허가번호_기관_코드": str,
    "허가번호_기관_코드_명": str,
    "허가번호_구분_코드": str,
    "허가번호_구분_코드_명": str,
    "호_수(호)": "Int64",
    "에너지효율_등급": str,
    "에너지절감_율": float,
    "에너지_EPI점수": "Int64",
    "친환경_건축물_등급": str,
    "친환경_건축물_인증점수": "Int64",
    "지능형_건축물_등급": str,
    "지능형_건축물_인증점수": "Int64",
    "생성_일자": str,
}

# [건축물대장] 표제부 (동별개요)

# 28710-28000|1|일반|2|일반건축물|
# 인천광역시 강화군 강화읍 00리 000번지| 인천광역시 강화군 강화읍 000길 00-0||
# 28000|25000|0|0123|0000||||0|287104000000|25000|0|999|99||0|주건축물|
# 110|60.001|50.01|130.01|130.01|110.1|
# 11|벽돌구조|벽돌구조|01000|단독주택|제2종근린생활시설|10|(철근)콘크리트|(철근)콘크리트|
# 0|1|0|2|0|0|0|0|0|130.01|0|0|0|0|0|0|0|0|
# 19990109||19910123||||||0||0|0||0||0|20120304||

title_dtypes = {
    "관리_건축물대장_PK": str,
    "대장_구분_코드": str,
    "대장_구분_코드_명": str,
    "대장_종류_코드": str,
    "대장_종류_코드_명": str,
    "대지_위치": str,
    "도로명_대지_위치": str,
    "건물_명": str,
    "시군구_코드": str,
    "법정동_코드": str,
    "대지_구분_코드": str,
    "번": str,
    "지": str,
    "특수지_명": str,
    "블록": str,
    "로트": str,
    "외필지_수": "Int64",
    "새주소_도로_코드": str,
    "새주소_법정동_코드": str,
    "새주소_지상지하_코드": str,
    "새주소_본_번": "Int64",
    "새주소_부_번": "Int64",
    "동_명": str,
    "주_부속_구분_코드": str,
    "주_부속_구분_코드_명": str,
    "대지_면적(㎡)": float,
    "건축_면적(㎡)": float,
    "건폐_율(%)": float,
    "연면적(㎡)": float,
    "용적_률_산정_연면적(㎡)": float,
    "용적_률(%)": float,
    "구조_코드": str,
    "구조_코드_명": str,
    "기타_구조": str,
    "주_용도_코드": str,
    "주_용도_코드_명": str,
    "기타_용도": str,
    "지붕_코드": str,
    "지붕_코드_명": str,
    "기타_지붕": str,
    "세대_수(세대)": "Int64",
    "가구_수(가구)": "Int64",
    "높이(m)": float,
    "지상_층_수": "Int64",
    "지하_층_수": "Int64",
    "승용_승강기_수": "Int64",
    "비상용_승강기_수": "Int64",
    "부속_건축물_수": "Int64",
    "부속_건축물_면적(㎡)": float,
    "총_동_연면적(㎡)": float,
    "옥내_기계식_대수(대)": "Int64",
    "옥내_기계식_면적(㎡)": float,
    "옥외_기계식_대수(대)": "Int64",
    "옥외_기계식_면적(㎡)": float,
    "옥내_자주식_대수(대)": "Int64",
    "옥내_자주식_면적(㎡)": float,
    "옥외_자주식_대수(대)": "Int64",
    "옥외_자주식_면적(㎡)": float,
    "허가_일": str,
    "착공_일": str,
    "사용승인_일": str,
    "허가번호_년": str,
    "허가번호_기관_코드": str,
    "허가번호_기관_코드_명": str,
    "허가번호_구분_코드": str,
    "허가번호_구분_코드_명": str,
    "호_수(호)": "Int64",
    "에너지효율_등급": str,
    "에너지절감_율": float,
    "에너지_EPI점수": "Int64",
    "친환경_건축물_등급": str,
    "친환경_건축물_인증점수": "Int64",
    "지능형_건축물_등급": str,
    "지능형_건축물_인증점수": "Int64",
    "생성_일자": str,
    "내진_설계_적용_여부": str,
    "내진_능력": str,
}

# 건축물대장 층별개요

floor_dtypes = {
    "관리_건축물대장_PK": str,
    "대지_위치": str,
    "도로명_대지_위치": str,
    "건물_명": str,
    "시군구_코드": str,
    "법정동_코드": str,
    "대지_구분_코드": str,
    "번": str,
    "지": str,
    "특수지_명": str,
    "블록": str,
    "로트": str,
    "새주소_도로_코드": str,
    "새주소_법정동_코드": str,
    "새주소_지상지하_코드": str,
    "새주소_본_번": "Int64",
    "새주소_부_번": "Int64",
    "동_명": str,
    "층_구분_코드": str,
    "층_구분_코드_명": str,
    "층_번호": "Int64",
    "층_번호_명": str,
    "구조_코드": str,
    "구조_코드_명": str,
    "기타_구조": str,
    "주_용도_코드": str,
    "주_용도_코드_명": str,
    "기타_용도": str,
    "면적(㎡)": float,
    "주_부속_구분_코드": str,
    "주_부속_구분_코드_명": str,
    "면적_제외_여부": str,
    "생성_일자": str,
}

# 건축물대장 전유부 exclusive possession

expos_dtypes = {
    "관리_건축물대장_PK": str,
    "대장_구분_코드": str,
    "대장_구분_코드_명": str,
    "대장_종류_코드": str,
    "대장_종류_코드_명": str,
    "대지_위치": str,
    "도로명_대지_위치": str,
    "건물_명": str,
    "시군구_코드": str,
    "법정동_코드": str,
    "대지_구분_코드": str,
    "번": str,
    "지": str,
    "특수지_명": str,
    "블록": str,
    "로트": str,
    "새주소_도로_코드": str,
    "새주소_법정동_코드": str,
    "새주소_지상지하_코드": str,
    "새주소_본_번": "Int64",
    "새주소_부_번": "Int64",
    "동_명": str,
    "호_명": str,
    "층_구분_코드": str,
    "층_구분_코드_명": str,
    "층_번호": "Int64",
    "생성_일자": str,
}

exposarea_dtypes = {
    "관리_건축물대장_PK": str,
    "대장_구분_코드": str,
    "대장_구분_코드_명": str,
    "대장_종류_코드": str,
    "대장_종류_코드_명": str,
    "대지_위치": str,
    "도로명_대지_위치": str,
    "건물_명": str,
    "시군구_코드": str,
    "법정동_코드": str,
    "대지_구분_코드": str,
    "번": str,
    "지": str,
    "특수지_명": str,
    "블록": str,
    "로트": str,
    "새주소_도로_코드": str,
    "새주소_법정동_코드": str,
    "새주소_지상지하_코드": str,
    "새주소_본_번": "Int64",
    "새주소_부_번": "Int64",
    "동_명": str,
    "호_명": str,
    "층_구분_코드": str,
    "층_구분_코드_명": str,
    "층_번호": "Int64",
    "전유_공용_구분_코드": str,
    "전유_공용_구분_코드_명": str,
    "주_부속_구분_코드": str,
    "주_부속_구분_코드_명": str,
    "층_번호_명": str,
    "구조_코드": str,
    "구조_코드_명": str,
    "기타_구조": str,
    "주_용도_코드": str,
    "주_용도_코드_명": str,
    "기타_용도": str,
    "면적(㎡)": float,
    "생성_일자": str,
}


def openeais_to_parquet(
    path, savedir, dtypes: Mapping, index="관리_건축물대장_PK", **kwargs
) -> Path:
    encoding = "cp949"
    header = None
    sep = "|"

    path = Path(path)
    savedir = Path(savedir)

    if savedir.exists() and savedir.stat().st_mtime > path.stat().st_mtime:
        print("already done")
        return savedir
    else:
        for file in savedir.iterdir():
            file.unlink()

    with TemporaryDirectory() as tdir:  # type: str
        ext = path.suffix
        if ext == ".zip":
            with zipfile.ZipFile(path) as zf:
                zf.extractall(
                    path=tdir, members=tqdm(zf.infolist(), desc="Extracting zip")
                )
            unzipped_paths = sorted(Path(tdir).glob("*.txt"))
            ddf: dd.DataFrame = dd.read_csv(
                unzipped_paths,
                encoding=encoding,
                header=header,
                sep=sep,
                names=dtypes.keys(),
                dtype=dtypes,
                **kwargs
            )
        elif ext == ".txt":
            ddf: dd.DataFrame = dd.read_csv(
                path,
                encoding=encoding,
                header=header,
                sep=sep,
                names=dtypes.keys(),
                dtype=dtypes,
                **kwargs
            )
        else:
            raise Exception("wrong file format")

        with ProgressBar():
            if index:
                print("replacing NA...")
                if dtypes[index] in ["string", str]:
                    replace_value = ""
                elif dtypes[index] in ["Int64", "int64", int]:
                    replace_value = 0
                elif dtypes[index] in ["float64", float]:
                    replace_value = 0.0
                else:
                    raise NotImplementedError(ddf[index].dtype)
                ddf = ddf.fillna({index: replace_value})

                print("Setting index...")
                ddf = ddf.set_index(index)
            print("Saving...")
            ddf.to_parquet(savedir)

    return savedir


if __name__ == "__main__":
    zip_patterns = [
        "국토교통부_건축물대장_기본개요*.zip",
        "국토교통부_건축물대장_총괄표제부*.zip",
        "국토교통부_건축물대장_표제부*.zip",
        "국토교통부_건축물대장_전유부*.zip",
        "국토교통부_건축물대장_층별개요*.zip",
        "국토교통부_건축물대장_전유공용면적*.zip",
    ]
    dtype_dicts = [
        bldrgst_dtypes,
        recap_dtypes,
        title_dtypes,
        floor_dtypes,
        expos_dtypes,
        exposarea_dtypes,
    ]
    savedir_names = [
        "bldrgst",
        "recap",
        "title",
        "floor",
        "expos",
        "exposarea",
    ]

    for zpattern, dtype, dirname in zip(zip_patterns, dtype_dicts, savedir_names):
        zpath = sorted(Path("data/").glob(zpattern), reverse=True)[0]
        openeais_to_parquet(zpath, Path("data") / dirname, dtype)
