################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Important classes of Flink Table API:

    - :class:`pyflink.table.TableEnvironment`
      Main entry point for :class:`Table` and SQL functionality
    - :class:`pyflink.table.Table`
      The core component of the Table API.
      Use the methods of :class:`Table` to transform data.
    - :class:`pyflink.table.TableConfig`
      A config to define the runtime behavior of the Table API.
      It is necessary when creating :class:`TableEnvironment`.
    - :class:`pyflink.table.StreamQueryConfig` and :class:`pyflink.table.BatchQueryConfig`
      A query config holds parameters to configure the behavior of queries.
    - :class:`pyflink.table.TableSource`
      Defines an external data source as a table.
    - :class:`pyflink.table.TableSink`
      Specifies how to emit a table to an external system or location.
    - :class:`pyflink.table.DataTypes`
      Defines a list of data types available.
    - :class:`pyflink.table.Row`
      A row in a :class:`Table`.
    - :class:`pyflink.table.window`
      Helper classes for working with :class:`pyflink.table.window.GroupWindow`
      (:class:`pyflink.table.window.Tumble`, :class:`pyflink.table.window.Session`,
      :class:`pyflink.table.window.Slide`) and :class:`pyflink.table.window.OverWindow` window
      (:class:`pyflink.table.window.Over`).
    - :class:`pyflink.table.table_descriptor`
      Helper classes that describes DDL information, such as how to connect to another system,
      the format of data, the schema of table, the event time attribute in the schema, etc.
    - :class:`pyflink.table.catalog`
      Responsible for reading and writing metadata such as database/table/views/UDFs
      from a registered :class:`pyflink.table.catalog.Catalog`.
    - :class:`pyflink.table.TableSchema`
      Represents a table's structure with field names and data types.
"""
from __future__ import absolute_import

from pyflink.table.query_config import BatchQueryConfig, StreamQueryConfig
from pyflink.table.table import Table, GroupedTable, GroupWindowedTable, OverWindowedTable,\
                                WindowGroupedTable
from pyflink.table.table_config import TableConfig
from pyflink.table.table_environment import TableEnvironment
from pyflink.table.table_sink import TableSink, CsvTableSink
from pyflink.table.table_source import TableSource, CsvTableSource
from pyflink.table.types import DataTypes, UserDefinedType, Row
from pyflink.table.table_schema import TableSchema

__all__ = [
    'TableEnvironment',
    'Table',
    'GroupedTable',
    'GroupWindowedTable',
    'OverWindowedTable',
    'WindowGroupedTable',
    'TableConfig',
    'StreamQueryConfig',
    'BatchQueryConfig',
    'TableSink',
    'TableSource',
    'CsvTableSink',
    'CsvTableSource',
    'DataTypes',
    'UserDefinedType',
    'Row',
    'TableSchema'
]
