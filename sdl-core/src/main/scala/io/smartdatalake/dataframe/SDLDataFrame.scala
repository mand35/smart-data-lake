/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package io.smartdatalake.dataframe

import com.snowflake.snowpark.TreeStringHelper

private[smartdatalake] trait SDLColumn {
  def column: Any
}

private[smartdatalake] case class SparkSDLColumn(override val column: SparkColumn) extends SDLColumn {}

private[smartdatalake] case class SnowparkSDLColumn(override val column: SnowparkColumn) extends SDLColumn {}

private[smartdatalake] trait SDLStructField {
  def name: String

  def dataType: SDLDataType
}

private[smartdatalake] case class SparkSDLStructField(column: SparkStructField) extends SDLStructField {
  override def name: String = column.name

  override def dataType: SDLDataType = SparkSDLDataType(column.dataType)
}

private[smartdatalake] case class SnowparkSDLStructField(column: SnowparkStructField) extends SDLStructField {
  override def name: String = column.name

  override def dataType: SDLDataType = SnowparkSDLDataType(column.dataType)
}

private[smartdatalake] trait SDLStructType {
  def fields: Seq[SDLStructField]

  def treeString: String
}

private[smartdatalake] case class SparkSDLStructType(structType: SparkStructType) extends SDLStructType {
  override def fields: Seq[SparkSDLStructField] = structType.fields.map(field => SparkSDLStructField(field))

  override def treeString: String = structType.treeString
}

private[smartdatalake] case class SnowparkSDLStructType(structType: SnowparkStructType) extends SDLStructType {
  override def fields: Seq[SnowparkSDLStructField] = structType.fields.map(field => SnowparkSDLStructField(field))

  override def treeString: String = TreeStringHelper.RichStructType(structType).publicTreeString
}

private[smartdatalake] trait SDLDataType {
  def dataType: Any
  def simpleString: String
}

private[smartdatalake] case class SparkSDLDataType(override val dataType: SparkDataType) extends SDLDataType {
  override def simpleString: String = {
    dataType.simpleString
  }
}

private[smartdatalake] case class SnowparkSDLDataType(override val dataType: SnowparkDataType) extends SDLDataType {
  override def simpleString: String = {
    dataType.typeName
  }
}

private[smartdatalake] trait SDLDataFrame {
  def columns: Seq[String]

  def select(colsToSelect: Seq[SDLColumn]): SDLDataFrame

  def select(colNamesToSelect: Array[String]): SDLDataFrame

  def schema: SDLStructType
}

private[smartdatalake] case class SparkSDLDataFrame(dataFrame: SparkDataFrame) extends SDLDataFrame {
  override def columns: Seq[String] = dataFrame.columns

  override def select(colsToSelect: Seq[SDLColumn]): SparkSDLDataFrame = {
    val colsToSelectTyped: Seq[SparkColumn] = colsToSelect
    val dfResult: SparkDataFrame = dataFrame.select(colsToSelectTyped: _*)
    SparkSDLDataFrame(dfResult)
  }

  override def select(colNamesToSelect: Array[String]): SDLDataFrame = {
    val dfResult: SparkDataFrame = dataFrame.select(colNamesToSelect.map(org.apache.spark.sql.functions.col): _*)
    SparkSDLDataFrame(dfResult)
  }

  override def schema: SDLStructType = {
    SparkSDLStructType(dataFrame.schema)
  }
}

private[smartdatalake] case class SnowparkSDLDataFrame(dataFrame: SnowparkDataFrame) extends SDLDataFrame {
  override def columns: Seq[String] = dataFrame.schema.map(column => column.name)

  override def select(colsToSelect: Seq[SDLColumn]): SnowparkSDLDataFrame = {
    val colsToSelectTyped: Seq[SnowparkColumn] = colsToSelect
    val dfResult: SnowparkDataFrame = dataFrame.select(colsToSelectTyped)
    SnowparkSDLDataFrame(dfResult)
  }

  override def select(colNamesToSelect: Array[String]): SDLDataFrame = {
    val columnsToSelect: Array[SnowparkColumn] = colNamesToSelect.map(com.snowflake.snowpark.functions.col)
    val dfResult: SnowparkDataFrame = dataFrame.select(columnsToSelect)
    SnowparkSDLDataFrame(dfResult)
  }

  override def schema: SDLStructType = {
    SnowparkSDLStructType(dataFrame.schema)
  }
}

