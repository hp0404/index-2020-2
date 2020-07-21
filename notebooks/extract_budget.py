# P02_006
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

   
ROOT = Path(__file__).resolve().parent.parent 
INPUTS_PATH = ROOT / "v0.5" / "inputs" 
LAND = (# коди, що пов'язані з податком на землю
    "18010500",
    "18010600",
    "18010700",
    "18010800",
    "18010900"
)
REGIONS = {
    "02000000000" : "Вінницька",
    "03000000000" : "Волинська",
    "04000000000" : "Дніпропетровська",
    "05000000000" : "Донецька",
    "06000000000" : "Житомирська",
    "07000000000" : "Закарпатська",
    "08000000000" : "Запорізька",
    "09000000000" : "Івано-Франківська",
    "10000000000" : "Київська",
    "11000000000" : "Кіровоградська",
    "12000000000" : "Луганська",
    "13000000000" : "Львівська",
    "14000000000" : "Миколаївська", 
    "15000000000" : "Одеська",
    "16000000000" : "Полтавська",
    "17000000000" : "Рівненська",
    "18000000000" : "Сумська",
    "19000000000" : "Тернопільська",
    "20000000000" : "Харківська",
    "21000000000" : "Херсонська",
    "22000000000" : "Хмельницька",
    "23000000000" : "Черкаська",
    "24000000000" : "Чернівецька",
    "25000000000" : "Чернігівська",
}

with open(ROOT / "psql_engine.txt", "r") as f:
    CONNECTION = f.read()
POPULATION = pd.read_excel(
    INPUTS_PATH / "P99" / "population_2019-10_clean.xls",
    index_col=0)["population"].to_dict()

def db_connect(query: str, conn: str):
    """ Напряму вантажить дані з бази. """
    db_connection = create_engine(conn)
    return pd.read_sql(query, db_connection)

def generate_income_query(year):
    
    query = f"""
        SELECT "ADMIN", "FIN_SOURCE", "INCO", "EXECUTED", "DATE"
        FROM "Budget"."dbo_OpenBudgetIncomes"
        WHERE "ADMIN" IN ('02000000000',
                          '03000000000',
                          '04000000000',
                          '05000000000',
                          '06000000000',
                          '07000000000',
                          '08000000000',
                          '09000000000',
                          '10000000000',
                          '11000000000',
                          '12000000000',
                          '13000000000',
                          '14000000000',
                          '15000000000',
                          '16000000000',
                          '17000000000',
                          '18000000000',
                          '19000000000',
                          '20000000000',
                          '21000000000',
                          '22000000000',
                          '23000000000',
                          '24000000000',
                          '25000000000')
          AND "DATE" >= '{year}-04-01'
          AND "DATE" <= '{year}-06-30';"""
    
    return query


def collect_incomes(connection):
    
    current, previous = generate_income_query(2020), generate_income_query(2019)
    
    df_current = db_connect(current, connection)
    df_previous = db_connect(previous, connection)
    
    # земля
    land = df_current.loc[df_current["INCO"].isin(LAND)] \
                     .groupby("ADMIN", as_index=False)["EXECUTED"].sum() \
                     .rename(columns={"EXECUTED": "Плата за землю (p2_05)"})
    
    # ПДФО
    pdfo = df_current.loc[df_current["INCO"].eq("11010000")] \
                     .groupby("ADMIN", as_index=False)["EXECUTED"].sum() \
                     .rename(columns={"EXECUTED": "Податок на дохід фіз осіб (для p2_01)"})
    
    # Загальний фонд, без оф. трансферт \ цей рік
    
    mask = df_current["FIN_SOURCE"].eq("C") & df_current["INCO"].str.contains("^[1235]0000000")
    wo_transfers = df_current.loc[mask] \
                             .groupby("ADMIN", as_index=False)["EXECUTED"].sum() \
                             .rename(columns={"EXECUTED": "Дохід без міжбюдж. трансфертів (p2_02)"})
    
    # Загальний фонд, без оф. трансферт \ попередній рік
    mask = df_previous["FIN_SOURCE"].eq("C") & df_previous["INCO"].str.contains("^[1235]0000000")
    wo_transfers_previous = df_previous.loc[mask] \
                                       .groupby("ADMIN", as_index=False)["EXECUTED"].sum() \
                                       .rename(columns={"EXECUTED": "Дохід без міжбюдж. трансфертів _ ПОПЕРЕДНІЙ РІК"})
    
    # agg
    budget_agg = pd.DataFrame(land["ADMIN"])
    for df in (land, pdfo, wo_transfers, wo_transfers_previous):
        budget_agg = budget_agg.merge(df)

    return budget_agg


def main():
    query_p2_4 = f"""
    SELECT "ADMIN", SUM("EXECUTED")
    FROM "Budget"."dbo_OpenBudgetExpenses"
    WHERE "ADMIN" IN ('02000000000',
                      '03000000000',
                      '04000000000',
                      '05000000000',
                      '06000000000',
                      '07000000000',
                      '08000000000',
                      '09000000000',
                      '10000000000',
                      '11000000000',
                      '12000000000',
                      '13000000000',
                      '14000000000',
                      '15000000000',
                      '16000000000',
                      '17000000000',
                      '18000000000',
                      '19000000000',
                      '20000000000',
                      '21000000000',
                      '22000000000',
                      '23000000000',
                      '24000000000',
                      '25000000000')
      AND "ECON" ~ '^3'
      AND "DATE" >= '2020-04-01'
      AND "DATE" <= '2020-06-01'
    GROUP BY "ADMIN";"""

    df1 = db_connect(query_p2_4, CONNECTION)
    df1 = df1.rename(columns={"sum": "Капітальні видатки (p2_04)"})
    df2 = collect_incomes(CONNECTION)
    
    result = pd.merge(df1, df2, on="ADMIN")
    result.insert(0, "Область", result["ADMIN"].map(REGIONS))
    result["Населення"] = result["Область"].map(POPULATION)
    result["Податки на одну особу (p2_01)"]  = result["Податок на дохід фіз осіб (для p2_01)"] / result["Населення"]
    
    result["Дохід без міжбюдж. трансфертів порівняно з минулим періодом (p2_03)"] = (
        result["Дохід без міжбюдж. трансфертів (p2_02)"] / result["Дохід без міжбюдж. трансфертів _ ПОПЕРЕДНІЙ РІК"]
    )
    
    (INPUTS_PATH / "P2").mkdir(parents=True, exist_ok=True)
    result.to_excel(INPUTS_PATH / "P2" / "P02_006.xlsx", index=False)
    
    
if __name__ == "__main__":
    main()