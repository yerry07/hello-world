from datetime import timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from functions.DAG008.helpers import *

default_args = {
    "owner": 'admin',
    "start_date": pendulum.datetime(2022, 10, 21, 15, 50, 0, tz="America/Lima"),
    
}
#schedule_interval="0 16 * * *"

with DAG(dag_id="DAG008_Report_valorizado",
         default_args=default_args,
         max_active_runs=4,
         schedule_interval=timedelta(hours=1)) as dag:

    start_task = DummyOperator(task_id="start_task")

    get_ReleasedProductsV2_task = PythonOperator(task_id="get_ReleasedProductsV2_task",
                                    python_callable=get_ReleasedProductsV2)
    
    get_ProductCategoryAssignments_task = PythonOperator(task_id="get_ProductCategoryAssignments_task",
                                python_callable=get_ProductCategoryAssignments)
    
    get_VendorsV3_task = PythonOperator(task_id="get_VendorsV3_task",
                                python_callable=get_VendorsV3)
    
    get_AllProducts_task = PythonOperator(task_id="get_AllProducts_task",
                                python_callable=get_AllProducts)
    
    get_InventSum_task = PythonOperator(task_id="get_InventSum_task",
                                python_callable=get_InventSum)
    
    generate_file_task = PythonOperator(task_id="generate_file_task",
                              python_callable=generate_file)
    
    delete_inputs_task = PythonOperator(task_id="delete_inputs_task",
                                python_callable=delete_inputs)

    end_task = DummyOperator(task_id="end_task")


start_task >> get_ReleasedProductsV2_task >>[get_AllProducts_task,
                                            get_ProductCategoryAssignments_task,
                                            get_VendorsV3_task,
                                            get_InventSum_task] >> generate_file_task >> delete_inputs_task >> end_task