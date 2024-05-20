from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import requests

from data_time_prepare import is_current_time_in_twilight
from data_time_binary import get_time_binary
from test_merge_to_predictions import merge_to_predictions
from make_prediction import make_prediction_from_latest_model
from get_current_predictions_from_db import get_current_predictions_from_db
from check_records_for_current_date_and_hour import check_records_for_current_date_and_hour
from PrepareDataForClient import PrepareDataForClient


app = FastAPI()

origins = [
    "http://localhost:63342",
    "http://127.0.0.1:8000",

]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Определение модели данных для запроса
class WeatherData(BaseModel):
    latitude: float
    longitude: float


def get_current_weather_from_api():
    url = "https://api.open-meteo.com/v1/forecast?latitude=55.0415&longitude=82.9346&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
    response = requests.get(url)
    weather_data = response.json()
    current_weather = weather_data["current"]
    return current_weather


async def make_prediction(data_of_current_state):
    merge_to_predictions(data_of_current_state)
    make_prediction_from_latest_model()


def determine_lighting_state(current_time):
    current_month = current_time.month
    current_hour = current_time.hour

    lighting_state = {'is_light_on': 0, 'is_light_off': 0}

    if current_month in [12, 1, 2]:
        lighting_on_start = 16
        lighting_on_end = 8
    elif current_month in [3, 4, 5]:
        lighting_on_start = 19
        lighting_on_end = 6
    elif current_month in [6, 7, 8]:
        lighting_on_start = 20
        lighting_on_end = 5
    else:
        lighting_on_start = 17
        lighting_on_end = 8

    if current_hour >= lighting_on_start or current_hour < lighting_on_end:
        lighting_state['is_light_on'] = 1
    else:
        lighting_state['is_light_off'] = 1

    return lighting_state


def determine_road_condition(data_weather: dict):
    current_date = datetime.now()

    anti_ice_period = (current_date.month == 11 and current_date.day >= 15) or (1 <= current_date.month <= 3) or (
            current_date.month == 4 and current_date.day <= 15)

    temperature = data_weather["temperature_2m"]
    precipitation = data_weather["precipitation"]
    snow = data_weather["snowfall"]
    humidity = data_weather["relative_humidity_2m"]
    weather_code = data_weather["weather_code"]
    road_surface_conditions = {'is_dusty': 0, 'is_chemical': 0, 'is_snowy': 0, 'is_icy_conditions': 0, 'is_wet': 0,
                               'is_dry': 0}
    if anti_ice_period:
        return "Обработанное противогололедными материалами"

    if weather_code in [71, 73, 75, 77, 85, 86]:  # Коды для снега и снежных ливней
        if snow > 1:  # Если снегопад более 1 см
            road_surface_conditions['is_snowy'] = 1
            return road_surface_conditions
    elif weather_code in [66, 67]:  # Коды для замораживающего дождя
        road_surface_conditions['is_icy_conditions'] = 1
        return road_surface_conditions
    elif weather_code in [61, 63, 65, 80, 81, 82]:  # Коды для дождя и дождевых ливней
        if temperature < 0:
            road_surface_conditions['is_icy_conditions'] = 1
            return road_surface_conditions
        elif precipitation > 10:
            road_surface_conditions['is_wet'] = 1
            return road_surface_conditions
        elif precipitation < 10:
            road_surface_conditions['is_dry'] = 1
            return road_surface_conditions
    elif weather_code in [45, 48]:  # Коды для тумана
        road_surface_conditions['is_chemical'] = 1
        return road_surface_conditions
    elif weather_code in [0, 1, 2, 3]:  # Коды для ясного неба и облачности
        if humidity > 80:
            road_surface_conditions['is_chemical'] = 1
            return road_surface_conditions
        elif humidity < 50:
            road_surface_conditions['is_dry'] = 1
            return road_surface_conditions
        elif temperature > 30:
            road_surface_conditions['is_dusty'] = 1
            return road_surface_conditions
        else:
            road_surface_conditions['is_dry'] = 1
            return road_surface_conditions


def prepare_time_of_day_from_api():
    current_time = get_current_weather_from_api()
    time_of_day = {'is_daylight_hours': 0, 'is_dark_time': 0}

    if current_time['is_day']:
        time_of_day['is_daylight_hours'] = 1
    else:
        time_of_day['is_dark_time'] = 1

    return time_of_day


def get_day_off(date):
    response = requests.get(f'https://isdayoff.ru/{date}')
    return response.text == '1'


def prepare_weather_for_prediction(prediction_for_current_time):
    formatted_date = datetime.now().strftime('%Y-%m-%d')
    is_day_off = get_day_off(formatted_date)
    data_time = get_time_binary()
    time_of_day = prepare_time_of_day_from_api()

    city_name = "Novosibirsk"
    country_name = "Russia"
    latitude = 55.0415
    longitude = 82.9346

    if is_current_time_in_twilight(city_name, country_name, latitude, longitude):
        is_twilight = 1
        for key in time_of_day:
            time_of_day[key] = 0
    else:
        is_twilight = 0

    is_clear_sky = 0
    is_overcast = 0
    is_fog = 0
    is_rain = 0
    is_snow = 0
    is_snow_storm = 0
    is_hurricane_wind = 0
    temperature_is_above_30 = 0
    temperature_is_below_30 = 0
    current_weather = {}

    if prediction_for_current_time:
        current_weather = get_current_weather_from_api()

    road_surface_conditions = determine_road_condition(current_weather)

    match current_weather['weather_code']:
        case 0 | 1:
            is_clear_sky = 1
        case 2 | 3:
            is_overcast = 1
        case 45 | 48:
            is_fog = 1
        case 82 | 81 | 80 | 51 | 53 | 55 | 61 | 63 | 65 | 66 | 67:
            is_rain = 1
        case 71 | 73 | 75 | 77:
            is_snow = 1
        case _:
            pass

    if is_snow and current_weather['wind_speed_10m'] > 10:
        is_snow_storm = 1
    if current_weather['wind_speed_10m'] >= 15:
        is_hurricane_wind = 1
    if current_weather['wind_speed_10m'] >= 30:
        is_hurricane_wind = 1
    if current_weather['temperature_2m'] >= 30:
        temperature_is_above_30 = 1
    elif current_weather['temperature_2m'] <= -30:
        temperature_is_below_30 = 1

    data_time['isDayOff'] = is_day_off
    data_lighting_state = determine_lighting_state(datetime.now())

    if time_of_day['is_daylight_hours'] == 1:
        for item in data_lighting_state:
            data_lighting_state[item] = 0

    data_weather = {
        'is_dark_time': time_of_day['is_dark_time'],
        'is_daylight_hours': time_of_day['is_daylight_hours'],
        'is_twilight': is_twilight,
        'is_light_on': data_lighting_state['is_light_on'],
        'is_light_off': data_lighting_state['is_light_off'],
        'is_snow': is_snow,
        'is_fog': is_fog,
        'is_overcast': is_overcast,
        'is_clear_sky': is_clear_sky,
        'is_hurricane_wind': is_hurricane_wind,
        'is_rain': is_rain,
        'is_snow_storm': is_snow_storm,
        'temperature_is_above_30': temperature_is_above_30,
        'temperature_is_below_30': temperature_is_below_30,
    }

    print(f"[DATA] Погодные условия, дорожное покрытие и время успешно загружены")
    data_for_return = {**data_weather, **road_surface_conditions, **data_time}
    return data_for_return


# Асинхронная функция, которая будет выполняться каждые 5 минут
async def scheduled_task():
    data_of_current_state = prepare_weather_for_prediction(prediction_for_current_time=True)
    await make_prediction(data_of_current_state)


async def daily_scheduled_task():
    # try_to_parse_new_data_from_gibdd():
    print(f"[PARSE] Обучение модели отключено")


# Функция для запуска асинхронной задачи из планировщика
def run_async_task():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scheduled_task())
    loop.close()


def run_daily_async_task():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(daily_scheduled_task())
    loop.close()


# Настройка планировщика
scheduler = BackgroundScheduler()
scheduler.add_job(run_async_task, trigger=IntervalTrigger(hours=1))
scheduler.add_job(run_daily_async_task, trigger=IntervalTrigger(minutes=1))
scheduler.start()


# Запуск асинхронной задачи сразу после настройки планировщика
async def startup_event():
    asyncio.create_task(scheduled_task())


@app.on_event("startup")
async def on_startup():
    await startup_event()


# Закрытие планировщика при завершении работы приложения
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()


@app.get('/current-situation', response_model=dict)
async def predict():
    dict_of_current_state = get_current_predictions_from_db()
    dict_of_current_state = PrepareDataForClient(dict_of_current_state)
    return dict_of_current_state


@app.get("/check_predictions/")
async def check_predictions():
    if check_records_for_current_date_and_hour():
        return {"status": "success", "message": "There are records for the current date and hour."}
    else:
        return {"status": "success", "message": "No records found for the current date and hour."}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
