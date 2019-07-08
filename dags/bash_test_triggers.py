# Copyright © 2019 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the DAG...

# Create the default arguments
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'hashmap-airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 3),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# create the DAG instance
dag_layer_1 = DAG(dag_id='hw_bash_layer_1',
                  default_args=default_args,
                  schedule_interval=timedelta(1))

dag_layer_2 = DAG(dag_id='hw_bash_layer_2',
                  default_args=default_args,
                  schedule_interval=None)

dag_layer_3 = DAG(dag_id='hw_bash_layer_3',
                  default_args=default_args,
                  schedule_interval=None)

# Set start data

# These are passed in as args. Seems that they aren't sent that way is a bug.
dag_layer_1.start_date = default_args['start_date']
dag_layer_2.start_date = default_args['start_date']
dag_layer_3.start_date = default_args['start_date']

# This path is used in the code below. This should identify where the code is
# being executed from.
path = '/Users/johnaven/Sandbox/bash_dag_example'

# STDOUT 'Hello World' with redirect to out.txt
create_file = BashOperator(
    task_id='save-bash',
    bash_command='echo "Hello John" > {path}/out_tr.txt'.format(path=path)
)

# print the contents of out.txt to STDOUT
print_file = BashOperator(
    task_id='print-file',
    bash_command='cat {path}/out_tr.txt'.format(path=path)
)

# clone/copy the data into another file
copy_file = BashOperator(
    task_id='copy-file',
    bash_command='cp {path}/out_tr.txt {path}/out_tr_copy.txt'.format(path=path)
)

# delete the files that were created
delete_files = BashOperator(
    task_id='delete-files',
    bash_command='rm -f {path}/out_tr.txt && rm -f {path}/out_tr_copy.txt'.format(path=path)
)

# Create Triggers
trigger_layer_2 = TriggerDagRunOperator(
    task_id='trigger-layer2',
    trigger_dag_id='hw_bash_layer_2'
)

trigger_layer_3 = TriggerDagRunOperator(
    task_id='trigger-layer-3',
    trigger_dag_id='hw_bash_layer_3'
)

# Assign the operators to a DAG
create_file.dag = dag_layer_1
trigger_layer_2.dag = dag_layer_1

print_file.dag = dag_layer_2
copy_file.dag = dag_layer_2
trigger_layer_3.dag = dag_layer_2

delete_files.dag = dag_layer_3

# Set any upstream requirements - e.g. especially for the triggers
trigger_layer_2.set_upstream(task_or_task_list=[create_file])
trigger_layer_3.set_upstream(task_or_task_list=[print_file,
                                                copy_file])
