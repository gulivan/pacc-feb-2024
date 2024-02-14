import datetime
import pandas as pd
from random import randint
from typing import List, Dict
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
import httpx

SCHEDULE_CRON = "* * * * *"

class WeatherError(Exception):
    pass





@task(
        cache_key_fn=task_input_hash,
        cache_expiration=datetime.timedelta(minutes=2),
        retries=3,
        retry_delay_seconds=10
    )
def fetch_weather(lat: float = 38.9, lon: float = -77.0, target_variable: str = "temperature_2m") -> Dict[str, float]:
    # adding random error to try the retries
    if randint(0, 100) > 95:
        raise WeatherError("Random API error")
    base_url = "https://api.open-meteo.com/v1/forecast/"
    logger = get_run_logger()
    logger.info(f"Input params: lat: {lat}, lon {lon}")
    request_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": target_variable
    }

    temps = httpx.get(
        base_url,
        params=request_params
    )

    forecasted_temp = float(temps.json()["hourly"][target_variable][0])
    
    return {"lat": lat, "lon": lon, "time": datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), "condition": target_variable, "value": forecasted_temp}


@task(persist_result=True)
def save_to_csv(lat: float, lon: float, results: List[Dict[str, float]]):
    time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"weather_data_lat_{lat}_lon_{lon}_{time}.csv"
    df = pd.DataFrame(results)
    df.to_csv(filename, index=False)
    return df

@task(log_prints=True)
def save_artifact(results: List[Dict[str, float]]):
    time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    markdown_data = pd.DataFrame(results).to_markdown()
    artifact_key_name = f"weather-data-{time}"
    print(f"artifact_key_name = {artifact_key_name}")
    create_markdown_artifact(
        key=artifact_key_name,
        markdown=markdown_data,
        description="Weather data report"
    )


@flow()
def main(lat: float = 38.9, lon: float = -77.0, target_variables: str = "temperature_2m,relative_humidity_2m,wind_speed_10m"):
    logger = get_run_logger()
    target_variables = target_variables.split(",")
    results = []
    for target_variable in target_variables:
        results.append(fetch_weather(lat=lat, lon=lon, target_variable=target_variable))
    logger.info(results)
    save_to_csv(lat=lat, lon=lon, results=results)
    save_artifact(results=results)

    emit_event(event=f"flow.sent.event!", resource={"prefect.resource.id": f"{lat}-{lon}-{target_variables}"})

    


if __name__ == "__main__":
    # main()
    main.serve(name="deployment_3", cron=SCHEDULE_CRON, parameters=dict(lat=50, lon=77.0), tags=["weather", "1_day"])

# run deployment from the command line
# prefect deployment run fetch-weather/deployment_3 --param lon=77 --param lat=50