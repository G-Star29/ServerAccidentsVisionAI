import pandas as pd
import psycopg2
from pydantic import BaseModel

class YearlyData(BaseModel):
    year: int
    total_accidents: int
    dark_time_percentage: float
    light_time_percentage: float
    twilight_percentage: float
    snowfall_percentage: float
    fog_percentage: float
    cloudy_percentage: float
    clear_percentage: float
    hurricane_wind_percentage: float
    rain_percentage: float
    blizzard_percentage: float
    isDayOff_percentage: float
    morning_percentage: float
    afternoon_percentage: float
    evening_percentage: float
    night_percentage: float

def get_statistics_from_db():
    dbname = 'accidentsvisionai'
    user = 'postgres'
    password = 'Nikita232398'
    host = 'localhost'

    # Подключаемся к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host
    )
    cur = conn.cursor()

    select_query = """
    SELECT * FROM accidentvisionai.data_for_education_table
    WHERE col_4 = 1
    """

    cur.execute(select_query)
    result_of_cur = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    merged_df = pd.DataFrame(result_of_cur, columns=columns)

    select_columns_query = "SELECT column_number, column_name FROM accidentvisionai.columns_name_table;"
    cur.execute(select_columns_query)
    columns_data = cur.fetchall()
    columns_dict = {col_num: col_name for col_num, col_name in columns_data}
    merged_df.rename(columns=columns_dict, inplace=True)

    # Группировка данных по годам
    yearly_data = merged_df.groupby('year').size().reset_index(name='total_accidents')

    # Список условий для расчета процентов
    conditions = {
        'Темное время суток': 'в темное время суток',
        'Светлое время суток': 'светлое время суток',
        'Сумерки': 'сумерки',
        'Снегопад': 'Снегопад',
        'Туман': 'Туман',
        'Пасмурно': 'Пасмурно',
        'Ясно': 'Ясно',
        'Ураганный ветер': 'Ураганный ветер',
        'Дождь': 'Дождь',
        'Метель': 'Метель',
        'В выходные дни': 'isDayOff'
    }

    # Расчет процентов для стандартных условий
    for condition, column in conditions.items():
        condition_data = merged_df.groupby('year')[column].mean().reset_index(name=f'{column}_percentage')
        yearly_data = yearly_data.merge(condition_data, on='year')

    # Расчет процентов для времени суток
    time_conditions = {
        'morning': list(range(6, 12)),  # Утро с 6 до 11
        'afternoon': list(range(12, 18)),  # День с 12 до 17
        'evening': list(range(18, 24)),  # Вечер с 18 до 23
        'night': list(range(0, 6))  # Ночь с 0 до 5
    }

    for time_of_day, hours in time_conditions.items():
        merged_df[f'{time_of_day}_accidents'] = merged_df[[f'hour_{hour}' for hour in hours]].sum(axis=1)
        time_data = merged_df.groupby('year')[f'{time_of_day}_accidents'].mean().reset_index(name=f'{time_of_day}_percentage')
        yearly_data = yearly_data.merge(time_data, on='year')

    # Преобразование имен столбцов для соответствия модели
    column_mapping = {
        'в темное время суток_percentage': 'dark_time_percentage',
        'светлое время суток_percentage': 'light_time_percentage',
        'сумерки_percentage': 'twilight_percentage',
        'Снегопад_percentage': 'snowfall_percentage',
        'Туман_percentage': 'fog_percentage',
        'Пасмурно_percentage': 'cloudy_percentage',
        'Ясно_percentage': 'clear_percentage',
        'Ураганный ветер_percentage': 'hurricane_wind_percentage',
        'Дождь_percentage': 'rain_percentage',
        'Метель_percentage': 'blizzard_percentage',
        'isDayOff_percentage': 'isDayOff_percentage',
        'morning_percentage': 'morning_percentage',
        'afternoon_percentage': 'afternoon_percentage',
        'evening_percentage': 'evening_percentage',
        'night_percentage': 'night_percentage'
    }
    yearly_data.rename(columns=column_mapping, inplace=True)

    result = yearly_data.to_dict(orient='records')

    # Закрытие соединения
    cur.close()
    conn.close()

    return result
