from airflow import dag 
from airflow.providers.http.operators.http import SimpleHttpOperator 
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import daysago 
import json 

