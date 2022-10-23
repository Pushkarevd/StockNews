import pandas as pd
import tinvest as ti

from os import environ
from time import sleep
from datetime import datetime, timedelta


def get_figi(client: ti.SyncClient, ticker: str) -> str:
    return client.get_market_search_by_ticker(ticker).payload.instruments[0].figi


def get_figi_data(client: ti.SyncClient, figi: str, time_interval: datetime, day=0) -> pd.DataFrame:
    data = client.get_market_candles(
        figi=figi,
        from_=time_interval + timedelta(days=day),
        to=time_interval + timedelta(days=day + 1),
        interval=ti.CandleResolution.min15,
    ).payload  # Api doesn't count holidays
    dataframe = pd.DataFrame([{"close": float(step.c),
                               "high": float(step.h),
                               "low": float(step.l),
                               "open": float(step.o),
                               "time": step.time} for step in data.candles])
    return dataframe


def create_stock_data(client: ti.SyncClient, figi: str, ticker: str = None):
    START_TIME = datetime.strptime('01.10.2018', '%d.%m.%Y')
    END_TIME = datetime.now()
    DAYS = (END_TIME - START_TIME).days

    generator = (get_figi_data(client, figi, START_TIME, day) for day in range(DAYS))
    all_data = []
    partition_data = []
    for day, partition in enumerate(generator):
        partition_data.append(partition)

        if (day + 1) % 50 == 0:
            all_data.extend(partition_data)
            partition_data = []
            sleep(60)
        sleep(.5)

    all_data.extend(partition_data)
    pd.concat(all_data, ignore_index=True).to_csv(f"./data/{ticker if ticker else figi}.csv")


if __name__ == '__main__':
    token = environ.get('token')

    client = ti.SyncClient(token)

    ticker = 'AMZN'
    figi = get_figi(client, ticker)

    create_stock_data(client, figi, ticker)
