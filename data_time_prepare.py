from astral import LocationInfo
from astral.sun import sun, dusk, dawn
from timezonefinder import TimezoneFinder
from pytz import timezone, utc
from datetime import datetime, timedelta


def calculate_civil_twilight(city_name, country_name, latitude, longitude, date):
    # Определяем временную зону по координатам
    tf = TimezoneFinder()
    timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
    tz = timezone(timezone_str)

    # Определяем местоположение
    location = LocationInfo(city_name, country_name, timezone_str, latitude, longitude)

    # Получаем информацию о солнце на указанную дату
    s = sun(location.observer, date=date, tzinfo=tz)

    # Вычисляем времена начала и конца утренних и вечерних гражданских сумерек
    dawn_time = dawn(location.observer, date=date, tzinfo=tz)
    dusk_time = dusk(location.observer, date=date, tzinfo=tz)

    return {
        'morning_civil_twilight_start': dawn_time.astimezone(utc),
        'morning_civil_twilight_end': s['sunrise'].astimezone(utc),
        'evening_civil_twilight_start': s['sunset'].astimezone(utc),
        'evening_civil_twilight_end': dusk_time.astimezone(utc)
    }


def is_current_time_in_twilight(city_name, country_name, latitude, longitude):
    # Получаем текущее время и дату
    current_time = datetime.now(utc)
    current_date = current_time.date()

    # Вычисляем времена сумерек на текущую дату
    twilight_times = calculate_civil_twilight(city_name, country_name, latitude, longitude, current_date)

    # Проверяем, попадает ли текущее время в диапазон утренних или вечерних сумерек
    in_morning_twilight = twilight_times['morning_civil_twilight_start'] <= current_time <= twilight_times[
        'morning_civil_twilight_end']
    in_evening_twilight = twilight_times['evening_civil_twilight_start'] <= current_time <= twilight_times[
        'evening_civil_twilight_end']

    return in_morning_twilight or in_evening_twilight
