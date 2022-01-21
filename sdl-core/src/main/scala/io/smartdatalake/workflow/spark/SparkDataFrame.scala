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

package io.smartdatalake.workflow.spark

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.dataframe.{GenericColumn, GenericDataFrame, GenericDataType, GenericRow, GenericSchema}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{DataFrameUtil, SchemaUtil}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.{Column, DataFrame, Encoder, Row, functions}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

case class SparkDataFrame(inner: DataFrame) extends GenericDataFrame {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def schema: SparkSchema = SparkSchema(inner.schema)
  override def join(other: GenericDataFrame, joinCols: Seq[String]): SparkDataFrame = {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.join(sparkOther.inner, joinCols))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${other.subFeedType.typeSymbol.name} in method join")
    }
  }
  override def select(columns: Seq[GenericColumn]): SparkDataFrame = {
    assert(columns.forall(_.subFeedType =:= subFeedType), s"Unsupported subFeedType(s) ${columns.filter(c => !(c.subFeedType =:= subFeedType)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method select")
    SparkDataFrame(inner.select(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def agg(columns: Seq[GenericColumn]): SparkDataFrame = {
    assert(columns.forall(_.subFeedType =:= subFeedType), s"Unsupported subFeedType(s) ${columns.filter(c => !(c.subFeedType =:= subFeedType)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method select")
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail:_*))
  }
  override def unionByName(other: GenericDataFrame): SparkDataFrame= {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.unionByName(sparkOther.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${other.subFeedType.typeSymbol.name} in method join")
    }
  }
  override def filter(expression: GenericColumn): SparkDataFrame = {
    expression match {
      case sparkExpr: SparkColumn => SparkDataFrame(inner.filter(sparkExpr.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${expression.subFeedType.typeSymbol.name} in method filter")
    }
  }
  override def collect: Seq[GenericRow] = inner.collect.map(SparkRow)
  override def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = {
    SparkSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }
  override def withColumn(colName: String, expression: GenericColumn): GenericDataFrame = {
    expression match {
      case sparkExpression: SparkColumn => SparkDataFrame(inner.withColumn(colName,sparkExpression.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${expression.subFeedType.typeSymbol.name} in method withColumn")
    }
  }
  override def createOrReplaceTempView(viewName: String): Unit = {
    inner.createOrReplaceTempView(viewName)
  }
}

case class SparkSchema(inner: StructType) extends GenericSchema {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = {
    val sparkSchema = schema.convertIfNeeded(subFeedType).asInstanceOf[SparkSchema]
    val caseSensitive = SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
    val missingCols = SchemaUtil.schemaDiff(inner, sparkSchema.inner,
      ignoreNullable = Environment.schemaValidationIgnoresNullability,
      deep = Environment.schemaValidationDeepComarison,
      caseSensitive = caseSensitive
    )
    if (missingCols.nonEmpty) Some(SparkSchema(StructType(missingCols.toSeq)))
    else None
  }
  override def columns: Seq[String] = inner.fieldNames
  override def sql: String = inner.sql
  override def add(colName: String, dataType: GenericDataType): GenericSchema = {
    val sparkDataType = dataType.convertIfNeeded(subFeedType).asInstanceOf[SparkDataType]
    SparkSchema(inner.add(StructField(colName, sparkDataType.inner)))
  }
  override def getEmptyDataFrame(implicit context: ActionPipelineContext): GenericDataFrame = {
    SparkDataFrame(DataFrameUtil.getEmptyDataFrame(inner)(context.sparkSession))
  }
  override def getDataType(colName: String): GenericDataType = SparkDataType(inner.apply(colName).dataType)
}

case class SparkColumn(inner: Column) extends GenericColumn {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def ===(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(sparkColumn.inner === inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method ===")
    }
  }
  override def and(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(sparkColumn.inner and inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method and")
    }
  }
  override def or(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(sparkColumn.inner or inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method or")
    }
  }
  override def isin(list: Any*): GenericColumn = SparkColumn(inner.isin(list))
  override def as(name: String): GenericColumn = SparkColumn(inner.as(name))
  override def cast(dataType: GenericDataType): GenericColumn = {
    dataType match {
      case sparkDataType: SparkDataType => SparkColumn(inner.cast(sparkDataType.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method or")
    }
  }
}

case class SparkDataType(inner: DataType) extends GenericDataType {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def isSortable: Boolean = Seq(StringType, LongType, IntegerType, ShortType, FloatType, DoubleType, TimestampType).contains(inner)
  override def typeName: String = inner.typeName
  override def sql: String = inner.sql
}

case class SparkRow(inner: Row) extends GenericRow {
  override def get(index: Int): Any = inner.get(index)
  override def getAs[T](index: Int): T = get(index).asInstanceOf[T]
}