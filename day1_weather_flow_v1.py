from typing import Literal
from prefect import flow, get_run_logger
import httpx

SCHEDULE_CRON = "* * * * *"
Variables = Literal["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]


@flow
def fetch_weather (lat: float = 38.9, lon: float = -77.0, target_variable: Variables = "temperature_2m"):
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
    print(temps.json()) # print json schema only in the worker logs
    if target_variable == "temperature_2m":
        logger.info(f"Forecasted 🌡️ temp C: {forecasted_temp} degrees")
    elif target_variable == "relative_humidity_2m":
        logger.info(f"Forecasted 💧 relative humidity: {forecasted_temp} %")
    elif target_variable == "wind_speed_10m":
        logger.info(f"Forecasted 💨 wind speed: {forecasted_temp} m/s")
    return forecasted_temp

if __name__ == "__main__":
    # fetch_weather# .serve(name="deployment_1", cron=SCHEDULE_CRON, parameters=dict(lat=50, lon=77.0))
