from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from random import randint
import os
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.python import PythonSensor


def db():
    conn = BaseHook.get_connection('pg')

def hello():
    print("Airflow")
def random_generator():
    filename = Variable.get("filename")
    try:
        with open(filename, "r+") as  f:
            for line in f:
                if line[0:1] == "=":
                    os.system('sed -i "$ d" {0}'.format(filename))
                    os.system('sed -i "$ d" {0}'.format(filename))               
                    break
    except IOError as e:
        print("Creating file:", filename)
    for i in range(1):
        lst = [randint(1000, 9999) for i in range(2)]
        with open(filename, "a") as  f:
            print(*lst, sep=" ", file=f)
def sum_calc():
    filename = Variable.get("filename")
    with open(filename, "r+") as  f:
        summ = [0, 0]
        for line in f:
            line = line.split()
            line = list(map(int, line))
            summ=map(sum, zip(summ,line))
        summ=list(summ)
        summ=summ[0]-summ[1]
        print("=========", file=f)
        print(summ, file=f)

def should_continue(**kwargs):
    filename = Variable.get("filename")
    os.path.exists(filename)
    

date_start=datetime.now() - timedelta(minutes=120)
date_end=datetime.now() + timedelta(minutes=6)
with DAG(dag_id="first_dag", start_date=date_start, max_active_runs=5, schedule="*/1 * * * *") as dag:
    bash_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task = PythonOperator(task_id="world", python_callable = hello)
    generator_task = PythonOperator(task_id="random_generator", python_callable = random_generator)
    sum_task = PythonOperator(task_id="sum_calc", python_callable = sum_calc)
        
    sens = PythonSensor(
       task_id='waiting_for_file',
       poke_interval=30,
       python_callable=lambda *args, **kwargs: should_continue(),
       dag=dag
       )
    bash_task >> python_task >> generator_task >> sum_task 
    sens >> sum_task