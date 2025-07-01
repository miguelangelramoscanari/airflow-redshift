# AIRFLOW con AWS RedShift. Data Enginner

Proyecto propio de ETL y Automatizacion de Datawarehuse usando flujo de trabajo con Airflow y AWS RedShift como BD. Utilizamos una API de cryptomonedas como Data.

## CONTENIDO - CODIGO FUENTE

- dags/cryptomoneda/[cryptomoneda.py](dags/cryptomoneda/cryptomoneda.py) : Flujo del AirFlow
- dags/cryptomoneda/[utils.py](dags/cryptomoneda/utils.py) : Detalle de cada servicio del flujo
  * etl: Realiza el proceso de ETL
  * load_datawarehouse : Carga a la BD AWS RedShift usando un carga incremental.
