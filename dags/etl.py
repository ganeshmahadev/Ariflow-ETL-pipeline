from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator 
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json 

with DAG(
    dag_id='Nasa_pod',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False

) as dag: 
    
    #1 check table if it doesn't exist
    @task 
    def create_table():
        postgres_hook=PostgresHook(postgres_conn_id='mypostgres_connection')

        create_table_query= '''

        CREATE TABLE IF NOT EXISTS nasaapipod(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)      
        );       

        '''
        postgres_hook.run(create_table_query)

    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint='planetary/apod', ## NASA API enpoint for APOD
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection
        response_filter=lambda response:response.json(), ## Convert response to json
    )

    

    ## Step 3: Transform the data(Pick the information that i need to save)
    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')

        }
        return apod_data



    #4 load the data into postgres sql


    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id='mypostgres_connection')

        ## Define the SQL Insert Query

        insert_query = """
        INSERT INTO nasaapipod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the SQL Query

        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']


     ))
    #5 verify data DBviewer



    #6 define task dependencies
       ## Extract
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output
    ## Transform
    transformed_data=transform_apod_data(api_response)
    ## Load
    load_data_to_postgres(transformed_data)
    


