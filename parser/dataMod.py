from functools import lru_cache
import pandas as pd
import requests

def process_weather_conditions(df):
    weather_conditions = set()
    for item in df['weather']:
        conditions = item.split('; ')
        weather_conditions.update(conditions)

    weather_conditions.discard('Температура выше +30С')
    weather_conditions.discard('Температура ниже -30С')

    for condition in weather_conditions:
        df[condition] = df['weather'].apply(lambda x: 1 if condition in x else 0)

    df['Температура выше +30С'] = df['weather'].apply(lambda x: 1 if 'Температура выше +30С' in x else 0)
    df['Температура ниже -30С'] = df['weather'].apply(lambda x: 1 if 'Температура ниже -30С' in x else 0)

    return df

def process_datetime_features(df):
    df['year'] = pd.to_datetime(df['datetime']).dt.year
    for i in range(1, 13):
        df[f'month_{i}'] = (pd.to_datetime(df['datetime']).dt.month == i).astype(int)
    for i in range(1, 32):
        df[f'day_{i}'] = (pd.to_datetime(df['datetime']).dt.day == i).astype(int)
    for i in range(7):
        df[f'weekday_{i}'] = (pd.to_datetime(df['datetime']).dt.weekday == i).astype(int)
    for i in range(24):
        df[f'hour_{i}'] = (pd.to_datetime(df['datetime']).dt.hour == i).astype(int)

@lru_cache(maxsize=None)
def is_day_off(date):
    response = requests.get(f'https://isdayoff.ru/{date}')
    return response.text == '1'

def apply_day_off_status(df):
    df['date'] = pd.to_datetime(df['datetime']).dt.strftime('%Y%m%d')
    df['isDayOff'] = df['date'].apply(is_day_off)
    df.drop('date', axis=1, inplace=True)

def main():
    df = pd.read_csv('parsed_output.csv')
    df['accident_occurred'] = 1
    df['ligth'] = df['ligth'].str.lower().str.strip()

    light_conditions = ["в темное время суток", "светлое время суток", "сумерки", "освещение включено", "освещение не включено"]
    for condition in light_conditions:
        df[condition] = df['ligth'].apply(lambda x: 1 if condition in x else 0)

    df = process_weather_conditions(df)

    road_columns = ["Пыльное", "Обработанное противогололедными материалами", "Заснеженное", "Гололедица", "Мокрое", "Сухое"]
    for column in road_columns:
        df[column] = df['road_conditions'].apply(lambda x: 1 if column in x else 0)

    nearby_columns = []
    with open('nearby', 'r', encoding='utf-8') as file:
        for item in file:
            nearby_columns.append(item.strip())

    for column in nearby_columns:
        df[column] = df['nearby'].apply(lambda x: 1 if pd.notna(x) and column in x else 0)

    # Добавление недостающих столбцов из предоставленной таблицы и сохранение порядка
    df_provided = pd.read_csv('concatenated_data_final_v2.csv')
    missing_columns = set(df_provided.columns) - set(df.columns)
    for column in missing_columns:
        df[column] = 0

    process_datetime_features(df)
    apply_day_off_status(df)

    df = df[df_provided.columns]  # Перестроение DataFrame в соответствии с порядком столбцов из предоставленной таблицы

   # df.drop(['road_conditions', 'datetime', 'ligth', 'weather', 'nearby'], axis=1, inplace=True)
    df.to_csv('output_modified_test_final.csv', index=False)

if '__main__' == __name__:
    main()
