/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SchemaUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object SparkLanguageImplementation {

  type SparkDataFrame = DataFrame
  type SparkColumn = Column
  type SparkStructType = StructType
  type SparkArrayType = ArrayType
  type SparkMapType = MapType
  type SparkStructField = StructField
  type SparkDataType = DataType
  type SparkLanguageType = Language[DataFrame, Column, StructType, DataType]

  val language: SparkLanguageType = new SparkLanguageType {

    override def join(left: DataFrame,
                      right: DataFrame,
                      joinCols: Seq[String]): DataFrame = {
      left.join(right, joinCols)
    }

    override def col(colName: String): Column = {
      org.apache.spark.sql.functions.column(colName)
    }

    override def lit(value: Any): Column = {
      org.apache.spark.sql.functions.lit(value)
    }

    override def select(dataFrame: DataFrame,
                        column: Column): DataFrame = {
      dataFrame.select(column)
    }

    override def filter(dataFrame: DataFrame,
                        column: Column): DataFrame = {
      dataFrame.filter(column)
    }

    override def and(left: Column,
                     right: Column): Column = {
      left.and(right)
    }

    override def or(left: Column,
                     right: Column): Column = {
      left.or(right)
    }

    override def ===(left: Column, right: Column): Column = {
      left === right
    }

    override def =!=(left: Column, right: Column): Column = {
      left =!= right
    }

    override def isin(c: Column, list: Any*): Column = {
      c.isin(list)
    }

    override def schema(dataFrame: DataFrame): StructType = {
      dataFrame.schema
    }

    override def columns(dataFrame: DataFrame): Seq[String] = {
      dataFrame.columns
    }

    override def unionByName(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
      dataFrame1.unionByName(dataFrame2)
    }

    override def diffSchema(left: SparkStructType, right: SparkStructType): Option[SparkStructType] = {
      val caseSensitive = SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
      val missingCols = SchemaUtil.schemaDiff(left, right,
        ignoreNullable = Environment.schemaValidationIgnoresNullability,
        deep = Environment.schemaValidationDeepComarison,
        caseSensitive = caseSensitive
      )
      if (missingCols.nonEmpty) Some(StructType(missingCols.toSeq))
      else None
    }

    override def schemaColumns(schema: SparkStructType): Seq[String] = schema.fieldNames

    override def schemaSql(schema: SparkStructType): String = schema.sql

    override def schemaAdd(schema: SparkStructType, colName: String, dataType: SparkDataType): SparkStructType = schema.add(colName, dataType)
  }
}