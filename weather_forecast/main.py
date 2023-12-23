from fastapi import FastAPI, Query
import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd
import requests

app = FastAPI()
# получение прогноза погоды
GFS_BASE = "https://nomads.ncep.noaa.gov/dods"


# функция обращается к серверу и собирает датасет из последних доступных массивов прогноза погоды
def get_gfs(date: dt.date, varlist: list, run: int = 0, hour: int = None, res: str = "0p25"):
    date_str = date.strftime("%Y%m%d")
    url_base = f"{GFS_BASE}/gfs_{res}/gfs{date_str}"

    response = requests.head(url_base)
    if response.status_code != 200:  # если на текущую дату не расчитана модель, то он смотрит по предыдущему дню
        date = date - dt.timedelta(days=1)
        date_str = date.strftime("%Y%m%d")
        url_base = f"{GFS_BASE}/gfs_{res}/gfs{date_str}"

    for run in reversed(range(0, 24,
                              6)):  # модели расчитываются в 0, 6, 12, 18 часов. Поэотму идет итерация от самой поздней с шагом 6 часов
        url = f"{url_base}/gfs_{res}_{run:02d}z"
        try:
            with xr.open_dataset(url) as ds:
                if hour is None:
                    dataset = ds[varlist]
                else:
                    time = dt.time(hour=hour)
                    dataset = ds[varlist].sel(
                        time=dt.datetime.combine(date, time), method="nearest"
                    )
            # print(dataset)
            return dataset
        except:
            continue

    raise Exception("Data could not be retrieved for the specified date and run times")


# print(dt.datetime.now().date())
# print(get_gfs(date=dt.datetime.now().date(), varlist=['tmp2m'], run = 0, hour = None, res = "0p25"))

@app.get("/short_forecast")
async def get_forecast(
        lat: float = Query(..., alias="lat"),
        lon: float = Query(..., alias="lon")
):
    # Assume the forecast starts from today
    date = dt.date.today()

    variables = ["tmp2m", "pratesfc", "rh2m", "dswrfsfc", "ugrd10m", "vgrd10m"]

    # Get the data
    ds = get_gfs(date, variables)

    if (lat % 0.25 == 0) and (lon % 0.25 == 0):
        data = ds.sel(
            lat=lat,
            lon=lon
        )
    else:
        lat1 = (lat // 0.25) * 0.25
        lat2 = ((lat // 0.25) + 1) * 0.25
        lon1 = (lon // 0.25) * 0.25
        lon2 = ((lon // 0.25) + 1) * 0.25
        ds = ds.sel(lat=slice(lat1, lat2), lon=slice(lon1, lon2))
        data = ds.interp(
            lat=lat,
            lon=lon
        )

    # Calculate the wind speed and direction
    data['wind_speed'] = np.sqrt(data['ugrd10m'] ** 2 + data['vgrd10m'] ** 2)
    data['wind_dir'] = np.arctan2(data['ugrd10m'], data['vgrd10m']) * (180 / np.pi) + 180

    data = data.drop_vars(['ugrd10m', 'vgrd10m'])

    # Convert temperature from Kelvin to Celsius
    data['tmp2m'] = data['tmp2m'] - 273.15

    data = data.rename({
        'tmp2m': 'temp',
        'pratesfc': 'prec',
        'rh2m': 'rh',
        'dswrfsfc': 'rad'
    })
    data['time'] = data['time'] + pd.Timedelta(5.5, 'h')

    # Convert the data to a DataFrame, then to a dictionary
    data_df = data.to_dataframe().reset_index()
    data_df = data_df.iloc[1:]
    data_df['time'] = data_df['time'].dt.strftime('%Y-%m-%d %H:%M')
    data_df = data_df.reindex(columns=['lat', 'lon', 'time', 'temp', 'prec', 'rh', 'rad', 'wind_speed', 'wind_dir'])

    # data_df['rad'] = (data_df['rad'] * 10800) / 1000000
    response = data_df.to_dict(orient='records')

    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8092)
