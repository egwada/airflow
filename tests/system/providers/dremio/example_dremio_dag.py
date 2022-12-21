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
"""
Example Airflow DAG to execute SQL in an Dremio environment using the `DremioOperator`.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.dremio.operators.dremio import DremioOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_dremio_dag"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=["example" "dremio"],
) as dag:
    # [START howto_operator_dremio]
    sql_task = DremioOperator(
        task_id="create view ",
        sql="""
        CREATE OR REPLACE VIEW "test"."taxi_trips_year_month" AS 
        SELECT 
        EXTRACT(YEAR FROM pickup_datetime)  as pickup_year
        , EXTRACT(MONTH FROM pickup_datetime)  as pickup_month
        , SUM(tip_amount) AS Sum_tip_amount
        FROM Samples."samples.dremio.com"."NYC-taxi-trips"
        GROUP BY 1,2
        """,
    )
    # [END howto_operator_dremio]

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
5847854795884