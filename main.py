from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import base64


from parser.parserNewDataFromGIBDD import try_to_parse_new_data_from_gibdd
from EducationModel.model_training import create_new_classifier
from prepareWeather.prepare_weather import prepare_weather_for_prediction
from statisticsFromDB.get_statistics_from_db import get_statistics_from_db, YearlyData
from merge_to_predictions import merge_to_predictions_table
from make_prediction import make_prediction_from_latest_model
from get_current_predictions_from_db import get_current_predictions_from_db
from PrepareDataForClient import PrepareDataForClient
from statisticsFromDB.get_statistics_models import ModelData, get_model_data_from_db

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

async def make_prediction(data_of_current_state):
    merge_to_predictions_table(data_of_current_state, for_current_state=True)
    make_prediction_from_latest_model()

# Асинхронная функция, которая будет выполняться каждые 5 минут
async def scheduled_task():
    data_of_current_state = prepare_weather_for_prediction(prediction_for_current_time=True)
    await make_prediction(data_of_current_state)
    print(f"[PREDICTION] Отключено")


async def daily_scheduled_task():
   # status_code = try_to_parse_new_data_from_gibdd()
   # if not status_code:
   #     create_new_classifier()
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


@app.get('/next-hour-situation-{hours}', response_model=dict)
async def predict(hours: int):
    if hours < 1:
        raise HTTPException(status_code=400, detail="Number of hours must be at least 1")

    data_of_future_state = prepare_weather_for_prediction(prediction_for_current_time=False, hours=hours)
    dataFrame_for_predicion = merge_to_predictions_table(data_of_future_state, for_current_state=False)
    predictions = make_prediction_from_latest_model(for_current_state=False,
                                                    dataFrame_for_prediction=dataFrame_for_predicion)
    data_for_return = PrepareDataForClient(predictions)
    return data_for_return

@app.get("/get-statistics", response_model=list[YearlyData])
async def get_statistics():
    data_for_return = get_statistics_from_db()
    return data_for_return

@app.get("/get-predictions-models", response_model=list[ModelData])
async def get_statistics_models():
    return get_model_data_from_db()

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
