import pandas as pd
from tabulate import tabulate

df = pd.read_csv(
    "data/법정동코드 전체자료.txt",
    encoding="euc-kr",
    sep="\t",
    index_col=0,
    dtype={"법정동코드": str},
)
print(tabulate(df.head(), headers="keys"))
df.to_csv("data/code_bjd.csv")

df_sgg = df.loc[df.index.str.endswith("00000")]
df_sgg.index = df_sgg.index.str[:5]
df_sgg.index.name = "시군구코드"
df_sgg = df_sgg.rename(columns={"법정동명": "시군구명"})
print(tabulate(df_sgg.head(), headers="keys"))
df_sgg.to_csv("data/code_sgg.csv")

df_sido = df.loc[df.index.str.endswith("00000000")]
df_sido.index = df_sido.index.str[:2]
df_sido.index.name = "시도코드"
df_sido = df_sido.rename(columns={"법정동명": "시도명"})
print(tabulate(df_sido.head(), headers="keys"))
df_sido.to_csv("data/code_sido.csv")
