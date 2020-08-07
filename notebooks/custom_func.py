import numpy as np
import pandas as pd

INPUTS_PATH = "./../v0.5/inputs"
OUTPUT_PATH = "./../v0.5/outputs"

population = pd.read_excel(
    f"{INPUTS_PATH}/P99/population_2019-10_clean.xls",
    index_col=0)
dict_population = population.to_dict()["population"]
dict_regionid = {k:float(v) for k, v in population.to_dict()["region_id"].items()}

regions = {'region':
        ['Волинська',
         'Вінницька',
         'Дніпропетровська',
         'Донецька',
         'Житомирська',
         'Закарпатська',
         'Запорізька',
         'Київська',
         'Івано-Франківська',
         'Кіровоградська',
         'Луганська',
         'Львівська',
         'Миколаївська',
         'Одеська',
         'Полтавська',
         'Рівненська',
         'Сумська',
         'Тернопільська',
         'Харківська',
         'Херсонська',
         'Хмельницька',
         'Черкаська',
         'Чернівецька',
         'Чернігівська']}

# population = pd.read_excel(f"{INPUTS_PATH}/P99/population_kmu.xlsx", index_col=0)
# dict_population = population.to_dict()["Населення"]

# видалив відкриття df_KMU - неможливо перевідкрити таблицю без змін в межах одного notebook
df_KMU = pd.read_excel(f"{INPUTS_PATH}/P99/KMU.xlsx")

# # Визначення коефіцієнтів для функції нормалізації
def get_normal_coeffs(min_bound=0,max_bound=1):
    a = 1/(max_bound-min_bound)
    b = -a*min_bound
    return a,b

# # Нормалізація параметру нижнього рівня
def normalize_parameter(df, column_name, par_name, min_bound=True,max_bound=True,reverse=False):
    if isinstance(min_bound, bool):
        if reverse:
            min_bound = df[column_name].max()
        else:
            min_bound = df[column_name].min()
    if isinstance(max_bound, bool):
        if reverse:
            max_bound = df[column_name].min()
        else:
            max_bound = df[column_name].max()
    print(f'Емпіричні границі: {min_bound,max_bound}')
    
    slope, intercept = get_normal_coeffs(min_bound,max_bound)
    print(f'Коефіцієнти нормалізації: {slope, intercept}')
    
    df[par_name] = df[column_name].map(lambda x: slope*x+intercept)
    print(f'Параметр {par_name} нормалізовано і додано до таблиці\n')
    
# # Середнє зважене для розрахунку параметрів верхнього рівня та індексу
def weighted_average(row, dict_weights,columns):
    return sum([row[col]*dict_weights[col] for col in columns])/sum(dict_weights.values())

# # Створення та збереження остаточних датасетів по галузевому параметру
def save_data(sources,regions,dict_weights,parameter):
    df = pd.DataFrame(regions)
    df_raw = pd.DataFrame(regions)

    for source in sources:
        df = df.merge(source.loc[:, source.columns.str.contains("region|p\d{1}_\d{2}$")], on="region", how="outer")
        df_raw = df_raw.merge(source.loc[:, source.columns.str.contains("region|p\d{1}_\d{2}_raw$")], on="region", how="outer")
    
    df.fillna(0, inplace=True)
    cols = df.loc[:,df.columns.str.contains('p')].columns
    df[parameter] = df.loc[:,df.columns.str.contains('p')].apply(lambda x: weighted_average(x,dict_weights,cols),axis=1)*10
    
    df.to_csv(f"{OUTPUT_PATH}/calculated_index/{parameter}.csv", index=False)
    df_raw.to_csv(f"{OUTPUT_PATH}/calculated_index_raw/{parameter}_raw.csv", index=False)
    print(f'Дані за галузевим параметром {parameter} збережено')
    print(df)