# weather_etl(s)

## Freeze Check
Dag for checking for freezing weather on daily schedule. Notify when facuets need to be opened.


### Deployment
`scp -r -P 277 src/  zfreeze@rasp-pi:~/airflow/dags/weather_etl`
