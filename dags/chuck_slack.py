# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import os
from datetime import datetime

import airflow
import requests
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")


def send_slack_message(text):
    data = {"text": text}
    requests.post(
        url=SLACK_WEBHOOK,
        data=json.dumps(data),
        headers={"Content-type": "application/json"},
    )


def send_jokes(**context):
    jokes = []
    for i in range(3):
        joke = context["task_instance"].xcom_pull(task_ids=f"get_joke_{i}")
        jokes.append(joke)

    text = "\n".join(jokes)
    send_slack_message(text)


def send_final_message(**context):
    timestamp = datetime.today().strftime("%H:%M")
    text = f"Time now is: {timestamp}"
    send_slack_message(text)


with DAG(
    dag_id="chuck_to_slack",
    default_args={"start_date": airflow.utils.dates.days_ago(2)},
    schedule_interval="0 0 * * *",
) as dag:

    send_final_message_task = PythonOperator(
        task_id=f"send_final_message",
        python_callable=send_final_message,
        provide_context=True,
        dag=dag,
    )

    send_jokes_task = PythonOperator(
        task_id=f"send_jokes", python_callable=send_jokes, provide_context=True, dag=dag
    )

    for i in range(3):
        get_joke_task = BashOperator(
            task_id=f"get_joke_{i}",
            bash_command='curl -H "Accept: text/plain" https://api.chucknorris.io/jokes/random',
            dag=dag,
            xcom_push=True,
        )

        get_joke_task >> send_jokes_task

    send_jokes_task >> send_final_message_task
