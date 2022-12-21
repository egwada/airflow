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
from __future__ import annotations

from typing import Any, Iterable

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

from airflow.providers.common.sql.hooks.sql import DbApiHook


class DremioHook(DbApiHook):
    """
    Interact with Dremio via sqlalchemy-dremio.

    You can specify the SQLAlchemy dialect and driver that sqlalchemy-dremio
    will employ to communicate with Dremio in the extras field of your
    connection, e.g. ``{"dialect_driver": "dremio+flight"}`` for communication
    over Dremio's ODBC.  See the sqlalchemy-dremio documentation for
    descriptions of the supported dialects and drivers.
    
    sqlalchemy-dremio https://pypi.org/project/sqlalchemy-dremio/

    """

    conn_name_attr = "dremio_conn_id"
    default_conn_name = "dremio_default"
    conn_type = "dremio"
    hook_name = "Dremio"
    supports_autocommit = False

    def get_conn(self) -> Connection:
        """Establish a connection to Dremio."""
        conn_md = self.get_connection(getattr(self, self.conn_name_attr))
        creds = f"{conn_md.login}:{conn_md.password}@" if conn_md.login else ""
        engine = create_engine(
            f'{conn_md.extra_dejson.get("dialect_driver", "dremio+flight")}://{creds}'
            f"{conn_md.host}:{conn_md.port}/"
            f'{conn_md.schema}"?useEncryption=false"'
        )

        """engine = create_engine(
            'dremio+flight://****:*****@localhost:32010/dremio?useEncryption=false'
        )"""

        self.log.info(
            "Connected to the Dremio at %s:%s as user %s", conn_md.host, conn_md.port, conn_md.login
        )

        return engine.raw_connection()

    def get_uri(self) -> str:
        """
        Returns the connection URI

        e.g: ``dremio+flight://dremio:dremio123@localhost:32010/dremio``
        """
        conn_md = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn_md.host
        if conn_md.port is not None:
            host += f":{conn_md.port}"
        conn_type = conn_md.conn_type or "dremio"
        dialect_driver = conn_md.extra_dejson.get("dialect_driver", "dremio+flight")
        conn_schema = conn_md.schema
        return f"{dialect_driver}://{host}/{conn_schema}?useEncryption=false"

    def set_autocommit(self, conn: Connection, autocommit: bool) -> NotImplementedError:
        raise NotImplementedError("There are no transactions in Dremio.")

