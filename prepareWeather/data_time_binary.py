from datetime import datetime, timedelta


def get_time_binary(is_for_current_state=True, hour=0):

    if is_for_current_state:
        data_time = datetime.now()
    else:
        data_time = datetime.now()
        data_time = data_time + timedelta(hours=hour)

    date_dict = {}

    date_dict['year'] = data_time.year

    for month in range(1, 13):
        date_dict[f'month_{month}'] = 1 if data_time.month == month else 0

    for day in range(1, 32):
        date_dict[f'day_{day}'] = 1 if data_time.day == day else 0

    for weekday in range(7):
        date_dict[f'weekday_{weekday}'] = 1 if data_time.weekday() == weekday else 0

    for hour in range(24):
        date_dict[f'hour_{hour}'] = 1 if data_time.hour == hour else 0

    return date_dict
