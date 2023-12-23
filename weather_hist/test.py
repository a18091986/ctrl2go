import pandas as pd
import dotenv
import requests
import json
from typing import Tuple

env = dotenv.dotenv_values('../.env')
auth_key = env.get("AUTH_TOKEN")


def get_history_weather(datamode: str = 'bilinear', src_id: int = 2,
                        geo_points: Tuple[Tuple[float | int, float | int], ...] = ((28.632854, 77.219721), ),
                        start_date: str = "1990-01-01", end_date: str = "1990-01-03",
                        out_format: str = 'csv') -> pd.DataFrame | None:
    """

    Parameters
    ----------
    datamode
    src_id
    geo_points
    start_date
    end_date
    out_format

    Returns
    -------

    """
    weather_hist = pd.DataFrame()
    base_url = "http://212.41.22.36:8084/meteo/api/v1/webportal/ptDailyMeteoData"
    parameters = f"srcId={src_id}&dataMode={datamode}&startDate={start_date}&endDate={end_date}"
    url = '?'.join([base_url, parameters])

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {auth_key}'
    }

    # формируем тело запроса из набора координат
    geo_points_dict = list(map(lambda x: dict(latitude=x[0], longitude=x[1]), geo_points))
    geo_points_json = json.dumps(geo_points_dict)

    response = requests.request("POST", url, headers=headers, data=geo_points_json)

    if response.status_code != 200:
        print(f"Error: Unable to fetch meteo data. {response.content}")
        return weather_hist

    weather_hist = pd.DataFrame.from_records(response.json())
    weather_hist = weather_hist.loc[:, ~weather_hist.columns.isin(['sourceId', 'isInterpolated'])]

    if out_format == 'xlsx':
        weather_hist.to_excel('test.xlsx')
    elif out_format == 'sql':
        pass

    return weather_hist


geo_points = (
    (21.883583, 77.205752),
    (23.883583, 80.205752),
    (17.883583, 74.205752)
)


weather = get_history_weather(geo_points=geo_points, start_date='1990-01-01', end_date='1990-02-01', out_format='xlsx')
weather = weather.set_index(['latitude', 'longitude'])
print(weather.head(10).to_string())

# print(weather.groupby(['latitude', 'longitude']).to_string())
