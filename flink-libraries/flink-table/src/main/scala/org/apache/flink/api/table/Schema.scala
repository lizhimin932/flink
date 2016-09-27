/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.table

import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * A Schema represents a Table's structure
  */
class Schema(
  val columnNames: Array[String],
  val columnTypes: Array[TypeInformation[_]]) {

  if (columnNames.length != columnTypes.length) {
    throw new TableException(
      "Number of column indexes and column names must be equal.")
  }

  // check uniqueness of field names
  if (columnNames.toSet.size != columnTypes.length) {
    throw new TableException(
      "Table column names must be unique.")
  }

  val columnNameToIndex: Map[String, Int] = columnNames.zipWithIndex.toMap

  /**
    * Returns all type informations as an array
    */
  def getTypes: Array[TypeInformation[_]] = columnTypes

  /**
    * Returns the specified type information for the given column index
 *
    * @param columnIndex the index of the field
    */
  def getType(columnIndex: Int): TypeInformation[_] = columnTypes(columnIndex)

  /**
    * Returns the specified type information for the given column name
 *
    * @param columnName the name of the field
    */
  def getType(columnName: String): TypeInformation[_] = {
    if (columnNameToIndex.contains(columnName)) {
      columnTypes(columnNameToIndex(columnName))
    } else {
      throw FieldNotFoundException(s"Table doesn't have the column : $columnName")
    }
  }

  /**
    * Returns all column names as an array.
    */
  def getColumnNames: Array[String] = columnNames

  /**
    * Return the specified column name for the given column index.
 *
    * @param columnIndex the index of the field
    */
  def getColumnName(columnIndex: Int): String = columnNames(columnIndex)


  override def toString = {
    val builder = new StringBuilder
    builder.append("root\n")
    columnNames.zip(columnTypes).foreach{ case (name, typeInfo) =>
      builder.append(s" |-- $name: $typeInfo\n")
    }
    builder.toString()
  }

}
