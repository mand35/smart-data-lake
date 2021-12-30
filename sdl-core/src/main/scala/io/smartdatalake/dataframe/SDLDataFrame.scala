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

import com.snowflake.snowpark.SnowparkHelper
import org.apache.spark.sql.DatasetHelper

private[smartdatalake] trait SDLDataFrame {
  def showString(): String

  def dataFrame: Any

  def apply(colName: String): SDLColumn

  def lit(value: Any): SDLColumn

  def columns: Seq[String]

  def where(colToSelect: SDLColumn): SDLDataFrame = filter(colToSelect)

  def filter(column: SDLColumn): SDLDataFrame

  def select(first: String, remaining: String*): SDLDataFrame = {
    select((Seq(first) ++ remaining).toArray)
  }

  def select(colsToSelect: SDLColumn*): SDLDataFrame

  def select(colNamesToSelect: Array[String]): SDLDataFrame

  def withColumn(colName: String, column: SDLColumn): SDLDataFrame

  def as(alias: String): SDLDataFrame

  def join(sdlDataFrame: SDLDataFrame, joinCol: SDLColumn, joinType: String): SDLDataFrame =
    join(sdlDataFrame, Seq(joinCol), joinType)

  def join(sdlDataFrame: SDLDataFrame, joinCols: Seq[SDLColumn], joinType: String): SDLDataFrame

  def union(sdlDataFrame: SDLDataFrame): SDLDataFrame

  def schema: SDLStructType

  def count: Long

  def array(columns: SDLColumn*): SDLColumn

  def struct(columns: SDLColumn*): SDLColumn

  def when(condition: SDLColumn, value: Any): SDLColumn

  def explode(column: SDLColumn, newColumnName: String, outer: Boolean): SDLDataFrame

  def drop(colName: String): SDLDataFrame

  def drop(column: SDLColumn): SDLDataFrame
}

private[smartdatalake] case class SparkSDLDataFrame(dataFrame: SparkDataFrame) extends SDLDataFrame {
  override def showString: String = DatasetHelper.showString(dataFrame)

  override def apply(colName: String): SDLColumn = dataFrame(colName)

  override def lit(value: Any): SDLColumn = dataFrame.lit(value)

  override def columns: Seq[String] = dataFrame.columns

  override def filter(column: SDLColumn): SDLDataFrame = dataFrame.filter(column.column.asInstanceOf[SparkColumn])

  override def select(colsToSelect: SDLColumn*): SDLDataFrame = dataFrame.select(colsToSelect.map(col => col.column.asInstanceOf[SparkColumn]): _*)

  override def select(colNamesToSelect: Array[String]): SDLDataFrame =
    dataFrame.select(colNamesToSelect.map(org.apache.spark.sql.functions.col): _*)

  override def withColumn(colName: String, column: SDLColumn): SDLDataFrame =
    dataFrame.withColumn(colName, column.column.asInstanceOf[SparkColumn])

  override def as(alias: String): SDLDataFrame = dataFrame.as(alias)

  override def join(sdlDataFrame: SDLDataFrame, joinCols: Seq[SDLColumn], joinType: String): SDLDataFrame =
    dataFrame.join(sdlDataFrame, joinCols, joinType)

  override def union(sdlDataFrame: SDLDataFrame): SDLDataFrame = dataFrame.union(sdlDataFrame)

  override def schema: SDLStructType = SparkSDLStructType(dataFrame.schema)

  override def count: Long = dataFrame.count

  override def array(columns: SDLColumn*): SDLColumn = {
    val sparkColumns: Seq[SparkColumn] = columns
    org.apache.spark.sql.functions.array(sparkColumns: _*)
  }

  override def struct(columns: SDLColumn*): SDLColumn = {
    val sparkColumns: Seq[SparkColumn] = columns
    org.apache.spark.sql.functions.struct(sparkColumns: _*)
  }

  def when(condition: SDLColumn, value: Any): SDLColumn = org.apache.spark.sql.functions.when(condition, value)

  override def explode(column: SDLColumn, newColumName: String, outer: Boolean): SDLDataFrame = {
    val explodedColumn =
      if (outer) {
        org.apache.spark.sql.functions.explode_outer(column)
      }
      else {
        org.apache.spark.sql.functions.explode_outer(column)
      }

    dataFrame.withColumn(newColumName, explodedColumn)
  }

  override def drop(colName: String): SDLDataFrame = dataFrame.drop(colName)

  override def drop(column: SDLColumn): SDLDataFrame = dataFrame.drop(column)
}

private[smartdatalake] case class SnowparkSDLDataFrame(dataFrame: SnowparkDataFrame) extends SDLDataFrame {
  override def showString: String = SnowparkHelper.RichDataFrame(dataFrame).publicShowString

  override def apply(colName: String): SDLColumn = dataFrame(colName)

  override def columns: Seq[String] = dataFrame.schema.map(column => column.name)

  override def filter(column: SDLColumn): SDLDataFrame = dataFrame.filter(column.column.asInstanceOf[SnowparkColumn])

  override def select(colsToSelect: SDLColumn*): SDLDataFrame = dataFrame.select(colsToSelect)

  override def select(colNamesToSelect: Array[String]): SDLDataFrame =
    dataFrame.select(colNamesToSelect.map(com.snowflake.snowpark.functions.col))

  override def withColumn(colName: String, column: SDLColumn): SDLDataFrame =
    dataFrame.withColumn(colName, column.column.asInstanceOf[SnowparkColumn])

  // DataFrame aliases are not in the Snowpark API (there is no DataFrame.as method).
  // I think renaming all columns with a prefix will achieve the same thing
  override def as(alias: String): SDLDataFrame = {
    val colNamesAndAliases: Seq[(String, String)] = dataFrame.schema.names.map(colName => (colName, s"$alias.$colName"))
    val renameColumnFunctions: Seq[SnowparkDataFrame => SnowparkDataFrame] = colNamesAndAliases.map(nameAndAlias =>
      (df: SnowparkDataFrame) => df.rename(nameAndAlias._2, df(nameAndAlias._1)))
    renameColumnFunctions.foldLeft(dataFrame) {
      (previousResult, renameColumnFunction) => renameColumnFunction(previousResult)
    }
  }

  override def join(sdlDataFrame: SDLDataFrame, joinCols: Seq[SDLColumn], joinType: String): SDLDataFrame =
    dataFrame.join(sdlDataFrame, joinCols, joinType)

  override def union(sdlDataFrame: SDLDataFrame): SDLDataFrame = dataFrame.union(sdlDataFrame)

  override def schema: SDLStructType = dataFrame.schema

  override def count: Long = dataFrame.count

  override def array(columns: SDLColumn*): SDLColumn =
    com.snowflake.snowpark.functions.array_construct(columns.map(column => column): _*)

  override def struct(columns: SDLColumn*): SDLColumn =
    com.snowflake.snowpark.functions.object_construct(columns.map(column => column): _*)

  def when(condition: SDLColumn, value: Any): SDLColumn =
    com.snowflake.snowpark.functions.when(condition, com.snowflake.snowpark.functions.lit(value))

  override def explode(column: SDLColumn, newColumnName: String, outer: Boolean): SDLDataFrame = {
    val dfJoined = dataFrame.join(com.snowflake.snowpark.tableFunctions.flatten,
      Map(
        "input" -> column.column.asInstanceOf[SnowparkColumn],
        "outer" -> com.snowflake.snowpark.functions.lit(outer),
        "mode" -> com.snowflake.snowpark.functions.lit("array")
      )
    )

    dfJoined.rename(newColumnName, dfJoined("VALUE")).drop("SEQ", "KEY", "PATH", "INDEX", "THIS")
  }

  override def lit(value: Any): SDLColumn = dataFrame.lit(value)

  override def drop(colName: String): SDLDataFrame = dataFrame.drop(colName)

  override def drop(column: SDLColumn): SDLDataFrame = dataFrame.drop(column)
}

