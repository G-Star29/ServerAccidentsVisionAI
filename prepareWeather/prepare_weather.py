from datetime import datetime, timedelta

import requests

from prepareWeather.data_time_binary import get_time_binary
from prepareWeather.data_time_prepare import is_current_time_in_twilight

hours_table_for_future_predictions = {
    1: 5,
    2: 9,
    3: 13,
    4: 17,
}


def get_current_weather_from_api(for_current_state=True):
    try:
        if for_current_state:
            url = "https://api.open-meteo.com/v1/forecast?latitude=55.0415&longitude=82.9346&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
        else:
            url = "https://api.open-meteo.com/v1/forecast?latitude=55.0415&longitude=82.9346&minutely_15=temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,weather_code,wind_speed_10m,is_day&timezone=auto&forecast_days=1&forecast_minutely_15=24"

        response = requests.get(url)
        response.raise_for_status()

        weather_data = response.json()

        if for_current_state:
            current_weather = weather_data.get("current", {})
        else:
            current_weather = weather_data.get("minutely_15", [])

        return current_weather

    except requests.exceptions.HTTPError as http_err:
        print(f"[DATA] ОШИБКА! HTTP: {http_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"[DATA] ОШИБКА! Запрос: {req_err}")
        return None
    except ValueError as json_err:
        print(f"[DATA] ОШИБКА! JSON: {json_err}")
        return None

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


def determine_road_condition(data_weather: dict, for_current_state=True, hour=0):
    if for_current_state:
        current_date = datetime.now()
        anti_ice_period = (current_date.month == 11 and current_date.day >= 15) or (1 <= current_date.month <= 3) or (
                current_date.month == 4 and current_date.day <= 15)
        temperature = data_weather["temperature_2m"]
        precipitation = data_weather["precipitation"]
        snow = data_weather["snowfall"]
        humidity = data_weather["relative_humidity_2m"]
        weather_code = data_weather["weather_code"]

    else:
        current_date = datetime.now()
        current_date = current_date + timedelta(hours=hour)
        needed_hour = hours_table_for_future_predictions[hour]
        anti_ice_period = (current_date.month == 11 and current_date.day >= 15) or (1 <= current_date.month <= 3) or (
                current_date.month == 4 and current_date.day <= 15)
        temperature = data_weather["temperature_2m"][needed_hour]
        precipitation = data_weather["precipitation"][needed_hour]
        snow = data_weather["snowfall"][needed_hour]
        humidity = data_weather["relative_humidity_2m"][needed_hour]
        weather_code = data_weather["weather_code"][needed_hour]

    road_surface_conditions = {'is_dusty': 0, 'is_chemical': 0, 'is_snowy': 0, 'is_icy_conditions': 0, 'is_wet': 0,
                               'is_dry': 0}
    if anti_ice_period:
        road_surface_conditions['is_chemical'] = 1
        return road_surface_conditions
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
        road_surface_conditions['is_wet'] = 1
        return road_surface_conditions
    elif weather_code in [0, 1, 2, 3, 95]:  # Коды для ясного неба и облачности
        if humidity > 80:
            road_surface_conditions['is_wet'] = 1
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


def prepare_time_of_day_from_api(is_for_current_state=True, hour=0):
    time_of_day = {'is_daylight_hours': 0, 'is_dark_time': 0}

    if is_for_current_state:
        current_time = get_current_weather_from_api(for_current_state=True)
        if current_time['is_day']:
            time_of_day['is_daylight_hours'] = 1
        else:
            time_of_day['is_dark_time'] = 1

        return time_of_day
    else:
        current_time = get_current_weather_from_api(for_current_state=False)
        needed_hour = hours_table_for_future_predictions[hour]
        if current_time['is_day'][needed_hour]:
            time_of_day['is_daylight_hours'] = 1
        else:
            time_of_day['is_dark_time'] = 1

        return time_of_day


def get_day_off(date):
    response = requests.get(f'https://isdayoff.ru/{date}')
    return response.text == '1'


def prepare_weather_for_prediction(prediction_for_current_time, hours=0):
    if prediction_for_current_time:
        formatted_date = datetime.now().strftime('%Y-%m-%d')
        is_day_off = get_day_off(formatted_date)
        data_time = get_time_binary(is_for_current_state=True)
        time_of_day = prepare_time_of_day_from_api(is_for_current_state=True)
    else:
        now_time = datetime.now() + timedelta(hours=hours)
        is_day_off = get_day_off(now_time.strftime('%Y-%m-%d'))
        data_time = get_time_binary(is_for_current_state=False, hour=hours)
        time_of_day = prepare_time_of_day_from_api(is_for_current_state=False, hour=hours)

    city_name = "Novosibirsk"
    country_name = "Russia"
    latitude = 55.0415
    longitude = 82.9346

    if is_current_time_in_twilight(city_name, country_name, latitude, longitude, hours):
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

    if prediction_for_current_time:
        current_weather = get_current_weather_from_api(for_current_state=True)
        road_surface_conditions = determine_road_condition(current_weather, for_current_state=True)
        weather_code = current_weather["weather_code"]
        wind_speed = current_weather["wind_speed_10m"]
        temperature = current_weather["temperature_2m"]
        rain = current_weather["rain"]

    else:
        needed_hour = hours_table_for_future_predictions[hours]
        current_weather = get_current_weather_from_api(for_current_state=False)
        road_surface_conditions = determine_road_condition(current_weather, for_current_state=False, hour=hours)
        weather_code = current_weather["weather_code"][needed_hour]
        wind_speed = current_weather["wind_speed_10m"][needed_hour]
        temperature = current_weather["temperature_2m"][needed_hour]
        rain = current_weather["rain"][needed_hour]

    match weather_code:
        case 0 | 1:
            is_clear_sky = 1
        case 2 | 3 | 95:
            is_overcast = 1
        case 45 | 48:
            is_fog = 1
        case 82 | 81 | 80 | 51 | 53 | 55 | 61 | 63 | 65 | 66 | 67:
            is_rain = 1
        case 71 | 73 | 75 | 77:
            is_snow = 1
        case _:
            pass

    if rain > 5:
        is_rain = 1

    if is_snow and wind_speed > 2.5:
        is_snow_storm = 1
    if wind_speed >= 8.3:
        is_hurricane_wind = 1
    if temperature >= 30:
        temperature_is_above_30 = 1
    elif temperature <= -30:
        temperature_is_below_30 = 1

    data_time['isDayOff'] = is_day_off
    data_lighting_state = determine_lighting_state(datetime.now() + timedelta(hours=hours))

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
