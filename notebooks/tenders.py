import pandas as pd
from pathlib import Path
from itertools import chain
from sqlalchemy import create_engine


ROOT = Path(__file__).resolve().parent.parent 
INPUTS_PATH = ROOT / "v0.5" / "inputs" 
CREDENTIALS = ROOT / "psql_engine.txt"
KAZNACHEJSTVO = INPUTS_PATH / "P99" / "rozporiadnyky_kaznachejstvo.xlsx"
REGIONS = {
     "2" : "Вінницька",
     "3" : "Волинська",
     "4" : "Дніпропетровська",
     "5" : "Донецька",
     "6" : "Житомирська",
     "7" : "Закарпатська",
     "8" : "Запорізька",
     "9" : "Івано-Франківська",
    "10" : "Київська",
    "11" : "Кіровоградська",
    "12" : "Луганська",
    "13" : "Львівська",
    "14" : "Миколаївська", 
    "15" : "Одеська",
    "16" : "Полтавська",
    "17" : "Рівненська",
    "18" : "Сумська",
    "19" : "Тернопільська",
    "20" : "Харківська",
    "21" : "Херсонська",
    "22" : "Хмельницька",
    "23" : "Черкаська",
    "24" : "Чернівецька",
    "25" : "Чернігівська",
}

with open(CREDENTIALS, "r") as f:
    CONNECTION = f.read()
    

def invert(d):
    return {v: k for k, values in d.items() for v in values}


def get_edrpous(region, df):
    """ Повертає зведений список кодів ЄДРПОУ,
    котрі є розпорядниками чи одержувачами:
    * обласного бюджету (код 3) +
    * держбюджету (код 1) та містять у назві паттерн: "обласної державної адміністрації"
    """
    
    tmp = df.loc[df["Код території"].eq(region)].copy()
    
    
    contains_pattern = tmp["Повне найменування"].str.contains(
        "обласної державної адміністрації", case=False
    )
    code_1 = tmp.loc[(tmp["Вид бюджету"].eq("1")) & (contains_pattern)]["ЄДРПОУ"] 
    code_3 = tmp.loc[tmp["Вид бюджету"].eq("3")]["ЄДРПОУ"]
    
    res = list(set(chain(code_1.tolist(), code_3.tolist())))
    return res


def select_edrpou():
    
    kazna = pd.read_excel(KAZNACHEJSTVO, dtype=str, skiprows=2)
    return {c: get_edrpous(c, kazna) for c in kazna["Код території"].unique()}
        

def db_connect(query: str, conn: str):
    
    db_connection = create_engine(conn)
    return pd.read_sql(query, db_connection)


def filter_table(df, d):
    
    df["tenderEndDate"] = pd.to_datetime(df["tenderEndDate"], utc=True).dt.tz_convert(None)
    df["Код області"] = df["organizationTaxId"].map(invert(d))
    df.insert(0, "Область", df["Код області"].map(REGIONS))
#     df.to_excel(INPUTS_PATH / "P2" / "P02_007_raw.xlsx", index=False)
    
    res = df.loc[df["Область"].notnull()].copy()
    res.rename(columns={"Область": "region"}, inplace=True)
    return res.drop("Код області", 1).sort_values(["tenderEndDate"])


def make_dataset():
    """Використовує коди ЄДРПОУ з OpenBudget. """
   
    tenders_query = """
    SELECT 
        "tenderId", 
        title, 
        status, 
        "procurementMethod", 
        "tenderEndDate",
        "tenderValue",
        "organizationTaxId"
    FROM "Prozorro"."dbo_BizTenders"
    WHERE 
        "timeCreate" <= '2020-06-30' AND 
        "timeCreate" >= '2020-04-01';
    """
    
    edrpous_query = """
    SELECT
       A."BudgetCode",
       A."EDRPOU",
       B."ShortRegionName" 
    FROM
       "Budget"."Dim_Disposers" A 
       LEFT JOIN
          "General"."Dim_Regions" B 
          ON A."RegionID" = B."RegionCode"
    WHERE RIGHT (A."BudgetCode", 8) = '00000000'
    """
    
    tenders = db_connect(tenders_query, CONNECTION)
    edrpous = db_connect(edrpous_query, CONNECTION)
    
    mapping = edrpous.set_index("EDRPOU")["ShortRegionName"].to_dict()
    tenders["region"] = tenders["organizationTaxId"].map(mapping)
    tenders["tenderEndDate"] = pd.to_datetime(tenders["tenderEndDate"], utc=True).dt.tz_convert(None)
    
    result = tenders.loc[tenders["region"].notnull()]
    result.to_excel(INPUTS_PATH / "P2" / "P02_007_disposers_timecreate.xlsx", index=False)


def main():
    
    q = """
    SELECT 
        "tenderId", 
        title, 
        status, 
        "timeCreate", 
        "dateModified",
        "procurementMethod", 
        "tenderEndDate",
        "tenderValue",
        "organizationTaxId"
    FROM "Prozorro"."dbo_BizTenders"
    WHERE 
        "dateModified" ~ '2020'
     """
 
    #"timeCreate" <= '2020-06-30' AND 
    #"timeCreate" >= '2020-04-01';

    df = db_connect(q, CONNECTION)
    d = select_edrpou()
    result = filter_table(df, d)
    result.to_excel(INPUTS_PATH / "P2" / "P02_007_dateModified.xlsx", index=False)
    
    
if __name__ == "__main__":
    make_dataset()