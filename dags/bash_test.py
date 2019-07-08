#
# Modifications © 2019 Hashmap, Inc
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
default_args = {
    'owner': 'hashmap-airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 3),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# create the DAG instance
dag = DAG('hw_bash', default_args=default_args, schedule_interval=timedelta(1))

# These are passed in as args. Seems that they aren't sent that way is a bug.
dag.start_date = default_args['start_date']

# This path is used in the code below. This should identify where the code is
# being executed from.
path = '/Users/johnaven/Sandbox/bash_dag_example'

# STDOUT 'Hello World' with redirect to out.txt
create_file= BashOperator(
    task_id='save-bash',
    bash_command='echo "Hello John" > {path}/out.txt'.format(path=path)
)

# print the contents of out.txt to STDOUT
print_file=BashOperator(
    task_id='print-file',
    bash_command='cat {path}/out.txt'.format(path=path)
)

# clone/copy the data into another file
copy_file=BashOperator(
    task_id='copy-file',
    bash_command='cp {path}/out.txt {path}/out_copy.txt'.format(path=path)
)

# delete the files that were created
delete_files = BashOperator(
    task_id='delete-files',
    bash_command='rm -f {path}/out.txt && rm -f {path}/out_copy.txt'.format(path=path)
)

# Assign the operators to a DAG
create_file.dag = dag
print_file.dag = dag
copy_file.dag = dag
delete_files.dag = dag

# Create the DAG topology
print_file.set_upstream(task_or_task_list=[create_file])
copy_file.set_upstream(task_or_task_list=[create_file])
delete_files.set_upstream(task_or_task_list=[print_file,
                                             copy_file])