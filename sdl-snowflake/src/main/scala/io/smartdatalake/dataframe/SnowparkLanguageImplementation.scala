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

import com.snowflake.snowpark.types.{ArrayType, DataType, StructField, StructType}
import com.snowflake.snowpark.{Column, DataFrame}

object SnowparkLanguageImplementation {

  type SnowparkCaseExpression = com.snowflake.snowpark.CaseExpr
  type SnowparkDataFrame = DataFrame
  type SnowparkColumn = Column
  type SnowparkStructType = StructType
  type SnowparkArrayType = ArrayType
  type SnowparkStructField = StructField
  type SnowparkDataType = DataType
  type SnowparkLanguageType = Language[DataFrame, Column, StructType, DataType]

  val language: SnowparkLanguageType = new SnowparkLanguageType {

    override def col(colName: String): Column = {
      com.snowflake.snowpark.functions.col(colName)
    }

    override def join(left: DataFrame,
                      right: DataFrame,
                      joinCols: Seq[String]): DataFrame = {
      left.join(right, joinCols)
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

    def or(left: Column, right: Column): Column = {
      left.or(right)
    }

    override def ===(left: Column, right: Column): Column = {
      left === right
    }

    override def =!=(left: Column, right: Column): Column = {
      left =!= right
    }

    override def isin(c: Column, list: Any*): Column = {
      // as snowpark does not support "isin" function, we have to concat "or" statements
      list.map(v => c === v).reduce(_ or _)
    }

    override def lit(value: Any): Column = {
      com.snowflake.snowpark.functions.lit(value)
    }

    override def schema(dataFrame: DataFrame): StructType = {
      dataFrame.schema
    }

    override def columns(dataFrame: DataFrame): Seq[String] = {
      dataFrame.schema.map(column => column.name)
    }

    override def unionByName(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
      // "union" function in Spark API has "unionAll" semantics, thats why we use Snowpark "unionAll" here
      dataFrame1.unionAllByName(dataFrame2)
    }
  }
}
