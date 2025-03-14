import logging
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from weather_api.api_wrapper import Point, GridPoint


# CONSTANTS
MIN_TEMP: int = 33 # If temp is forecasted to drop below this, notification will be sent


# Define default arguments
default_args: dict[str, any] = {
    'owner': 'zfreeze',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def send_discord_notification(msg: str) -> None:
    pass


def check_freezing_temps() -> None:

    # Define Point
    point: Point = Point.create_from_api(lat=36.35, lon=-94.28)
    gridpoint: GridPoint = GridPoint.create_from_point(point=point)
    
    # Get twelve hour forecast
    twelve_hr_forecast: pd.DataFrame = gridpoint.get_hourly_forecast_periods_df().head(12)

    # Get min / max temperature
    min_temp: int = twelve_hr_forecast['temperature'].min()
    max_temp: int = twelve_hr_forecast['temperature'].max()
    logging.info(f"{min_temp = } - {max_temp = }")

    # Determine if notification is required:
    if min_temp < MIN_TEMP:
        logging.info(f"Temperature Close to Freezing. Drip faucets!")
        # send_discord_notification(f"Cold temperatures tonight. Drip Faucets!\n {min_temp = } F \n {max_temp = } F")

    else:
        logging.info(f"Temperature Range Okay... Will Not Send Notification.")

# DAG definition using @dag decorator
@dag(
    start_date=datetime(2025, 3, 5, 14, 0, 0, tzinfo=pendulum.timezone("US/Central")),
    end_date=None,
    schedule=None,
    dag_id="freeze_check",
    dagrun_timeout=timedelta(minutes=15),
    catchup=False,
    description="Nightly Freeze Check",
    max_active_runs=1,
    tags=['Centerton', 'NOAA', 'Weather'],
    default_args=default_args
)
def check_freezing_temps_dag():

    @task
    def check_temps():
        check_freezing_temps()
    

    # Define task dependencies
    check_temps()

# Instantiate DAG
check_freezing_temps_dag()