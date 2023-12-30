from pathlib import Path
import dotenv

from weather_hist.funcs import get_weather_history_with_geo

if __name__ == '__main__':
    env = dotenv.dotenv_values('../.env')

    dir_to_save = Path("out_data")
    dir_to_save.mkdir(exist_ok=True)

    while True:
        ID_PROJECT = '12'
        AUTH_KEY = env.get("AUTH_TOKEN")
        START_DATE = '1990-01-01'
        END_DATE = '1990-01-04'

        df = get_weather_history_with_geo(project_id=ID_PROJECT, auth_key=AUTH_KEY, start_date=START_DATE,
                                          end_date=END_DATE, out_format='xlsx', path_to_save=dir_to_save)
        # time.sleep(3600)
        break
    df = df.set_index(['Latitude', 'Longitude'])
    print(df.head(10).to_string())
