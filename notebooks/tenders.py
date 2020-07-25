import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


ROOT = Path(__file__).resolve().parent.parent 
INPUTS_PATH = ROOT / "v0.5" / "inputs" 
CREDENTIALS = ROOT / "psql_engine.txt"


def db_connect(query: str, conn: str):
    """ Підключення до PostgreSQL бази """
    db_connection = create_engine(conn)
    return pd.read_sql(query, db_connection)


def make_dataset(conn):
    """Використовує коди ЄДРПОУ з OpenBudget. """
   
    tenders_query = """
    SELECT title,
           status,
           "tenderId",
           "procurementMethod",
           "tenderEndDate",
           "tenderValue",
           "organizationTaxId"
      FROM "Prozorro"."dbo_BizTenders"
     WHERE "timeCreate" <= '2020-06-30' 
       AND "timeCreate" >= '2020-04-01';
    """
    
    edrpous_query = """
    SELECT T1."BudgetCode",
           T1."EDRPOU",
           T2."ShortRegionName" 
      FROM "Budget"."Dim_Disposers" AS T1 
           LEFT JOIN "General"."Dim_Regions" AS T2
           ON T1."RegionID" = T2."RegionCode"
     WHERE RIGHT (T1."BudgetCode", 8) = '00000000';
    """
    
    tenders = db_connect(tenders_query, conn)
    edrpous = db_connect(edrpous_query, conn)
    
    mapping = edrpous.set_index("EDRPOU")["ShortRegionName"].to_dict()
    tenders["region"] = tenders["organizationTaxId"].map(mapping)
    tenders["tenderEndDate"] = pd.to_datetime(tenders["tenderEndDate"], utc=True).dt.tz_convert(None)
    
    result = tenders.loc[tenders["region"].notnull()]
    result.to_excel(INPUTS_PATH / "P2" / "P02_007.xlsx", index=False)

    
if __name__ == "__main__":

    with open(CREDENTIALS, "r") as f:
        connection = f.read()
    
    make_dataset(connection)