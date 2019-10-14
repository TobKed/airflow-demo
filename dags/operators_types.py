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
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator


with DAG(
    dag_id="operators_types",
    default_args={"start_date": airflow.utils.dates.days_ago(2)},
    schedule_interval="0 0 * * *",
) as dag:

    # GoogleCloudStorageCreateBucketOperator
    create_gcs_bucket = DummyOperator(task_id="create_gcs_bucket", dag=dag)

    # S3KeySensor
    wait_until_file_present_on_s3_bucket = DummyOperator(
        task_id="wait_until_file_present_on_s3_bucket", dag=dag
    )

    # S3ToGoogleCloudStorageTransferOperator
    transfer_s3_to_gcs = DummyOperator(task_id="transfer_s3_to_gcs", dag=dag)

    create_gcs_bucket >> wait_until_file_present_on_s3_bucket >> transfer_s3_to_gcs
