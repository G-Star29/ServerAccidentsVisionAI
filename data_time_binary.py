from datetime import datetime


def get_time_binary():
    now = datetime.now()

    date_dict = {}

    date_dict['year'] = now.year

    for month in range(1, 13):
        date_dict[f'month_{month}'] = 1 if now.month == month else 0

    for day in range(1, 32):
        date_dict[f'day_{day}'] = 1 if now.day == day else 0

    for weekday in range(7):
        date_dict[f'weekday_{weekday}'] = 1 if now.weekday() == weekday else 0

    for hour in range(24):
        date_dict[f'hour_{hour}'] = 1 if now.hour == hour else 0

    return date_dict
