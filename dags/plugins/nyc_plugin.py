from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
from sqlalchemy.orm import sessionmaker


class NycPersistDfDataToPostgresTable(BaseHook):
    
    def __init__(self, *args, **kwargs):
        print('Custom Hook invoked')
    
    def execute(self, postgres_conn_id, dataframe):
        dataframe = dataframe.dropna()
        print('Received dataframe for processing')
        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        Session = sessionmaker(engine)
        with Session() as session:
            for i in dataframe.index:
                try:

                    session.execute(f"""
                                    insert into taxi_data (airport_fee, dolocationid, pulocationid, ratecodeid, vendorid, congestion_surcharge, extra, fare_amount, improvement_surcharge, mta_tax, passenger_count, payment_type, store_and_fwd_flag, 
                                    tip_amount, tolls_amount, total_amount, tpep_dropoff_datetime, tpep_pickup_datetime, 
                                    trip_distance) values ({dataframe['Airport_fee'][i]}, {dataframe['DOLocationID'][i]}, {dataframe['PULocationID'][i]},
                                    {dataframe['RatecodeID'][i]}, {dataframe['VendorID'][i]}, {dataframe['congestion_surcharge'][i]},{dataframe['extra'][i]},
                                    {dataframe['fare_amount'][i]},{dataframe['improvement_surcharge'][i]},{dataframe['mta_tax'][i]},{dataframe['passenger_count'][i]},
                                    {dataframe['payment_type'][i]},'{dataframe['store_and_fwd_flag'][i]}', {dataframe['tip_amount'][i]},{dataframe['tolls_amount'][i]},
                                    {dataframe['total_amount'][i]},to_timestamp('{dataframe['tpep_dropoff_datetime'][i]}', 'YYYY-MM-DD- HH24:MI:ss'),to_timestamp('{dataframe['tpep_pickup_datetime'][i]}', 'YYYY-MM-DD- HH24:MI:ss'),{dataframe['trip_distance'][i]})""")
                except Exception as e:
                    print(e)
            session.commit()
            session.close()
        return True
    


class NycTaxiPluginPlugin(AirflowPlugin):
    name = 'nyc_plugin'
    operators = []
    sensors = []
    hooks = [NycPersistDfDataToPostgresTable]


