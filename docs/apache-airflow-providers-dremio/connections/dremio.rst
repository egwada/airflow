 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



.. _howto/connection:dremio:

Dremio Connection
=======================

The Dremio connection type configures a connection to Dremio via the sqlalchemy-dremio Python package.

Default Connection IDs
----------------------

Dremio hooks and operators use ``dremio_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host of the Dremio to connect to (HTTP, ODBC Arrow flight).

Port (required)
    The port of the Dremio server to connect to.

Database (required)
    The database you want to connect (by default dremio)

Extra (optional)
