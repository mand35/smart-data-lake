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

package io.smartdatalake.dataframe.snowflake

import com.snowflake.snowpark.{Column, DataFrame, RelationalGroupedDataFrame, Row}
import com.snowflake.snowpark.types.{DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.dataframe.{GenericColumn, GenericDataFrame, GenericDataType, GenericGroupedDataFrame, GenericRow, GenericSchema}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

case class SnowparkDataFrame(inner: DataFrame) extends GenericDataFrame {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def schema: SnowparkSchema = SnowparkSchema(inner.schema)
  override def join(other: GenericDataFrame, joinCols: Seq[String]): SnowparkDataFrame = {
    other match {
      case sparkOther: SnowparkDataFrame => SnowparkDataFrame(inner.join(sparkOther.inner, joinCols))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${other.subFeedType.typeSymbol.name} in method join")
    }
  }
  override def select(columns: Seq[GenericColumn]): SnowparkDataFrame = {
    assert(columns.forall(_.subFeedType =:= subFeedType), s"Unsupported subFeedType(s) ${columns.filter(c => !(c.subFeedType =:= subFeedType)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method select")
    SnowparkDataFrame(inner.select(columns.map(_.asInstanceOf[SnowparkColumn].inner)))
  }
  override def groupBy(columns: Seq[GenericColumn]): SnowparkGroupedDataFrame = {
    assert(columns.forall(_.subFeedType =:= subFeedType), s"Unsupported subFeedType(s) ${columns.filter(c => !(c.subFeedType =:= subFeedType)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method select")
    val sparkCols = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkGroupedDataFrame(inner.groupBy(sparkCols))
  }
  override def agg(columns: Seq[GenericColumn]): SnowparkDataFrame = {
    assert(columns.forall(_.subFeedType =:= subFeedType), s"Unsupported subFeedType(s) ${columns.filter(c => !(c.subFeedType =:= subFeedType)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method select")
    val sparkCols = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail:_*))
  }
  override def unionByName(other: GenericDataFrame): SnowparkDataFrame= {
    other match {
      case sparkOther: SnowparkDataFrame => SnowparkDataFrame(inner.unionByName(sparkOther.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${other.subFeedType.typeSymbol.name} in method join")
    }
  }
  override def filter(expression: GenericColumn): SnowparkDataFrame = {
    expression match {
      case sparkExpr: SnowparkColumn => SnowparkDataFrame(inner.filter(sparkExpr.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${expression.subFeedType.typeSymbol.name} in method filter")
    }
  }
  override def collect: Seq[GenericRow] = inner.collect.map(SnowparkRow)
  override def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = {
    SnowparkSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }
  override def withColumn(colName: String, expression: GenericColumn): GenericDataFrame = {
    expression match {
      case sparkExpression: SnowparkColumn => SnowparkDataFrame(inner.withColumn(colName,sparkExpression.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${expression.subFeedType.typeSymbol.name} in method withColumn")
    }
  }
  override def drop(colName: String): GenericDataFrame = SnowparkDataFrame(inner.drop(colName))
  override def createOrReplaceTempView(viewName: String): Unit = {
    inner.createOrReplaceTempView(viewName)
  }
}

case class SnowparkGroupedDataFrame(inner: RelationalGroupedDataFrame) extends GenericGroupedDataFrame {
  override def subFeedType: Type = typeOf[SnowparkSubFeed]
  override def agg(columns: Seq[GenericColumn]): SnowparkDataFrame = {
    assert(columns.forall(_.subFeedType =:= subFeedType), s"Unsupported subFeedType(s) ${columns.filter(c => !(c.subFeedType =:= subFeedType)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method agg")
    val sparkCols = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail:_*))
  }
}

case class SnowparkSchema(inner: StructType) extends GenericSchema {
  override def subFeedType: Type = typeOf[SnowparkSubFeed]
  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = {
    val snowparkSchema = schema.convertIfNeeded(subFeedType).asInstanceOf[SnowparkSchema]
    val missingCols = SchemaUtil.schemaDiff(this, snowparkSchema,
      ignoreNullable = Environment.schemaValidationIgnoresNullability,
      deep = Environment.schemaValidationDeepComarison
    )
    if (missingCols.nonEmpty) Some(SnowparkSchema(StructType.apply(missingCols.toSeq)))
    else None
  }
  override def columns: Seq[String] = inner.names
  override def sql: String = inner.toString // TODO: not sure if this is valid sql...
  override def add(colName: String, dataType: GenericDataType): GenericSchema = {
    val snowparkDataType = dataType.convertIfNeeded(subFeedType).asInstanceOf[SparkDataType]
    SnowparkSchema(inner.add(StructField(colName, snowparkDataType.inner)))
  }
  override def remove(colName: String): GenericSchema = {
    SnowparkSchema(StructType(inner.filterNot(_.name == colName)))
  }
  override def getEmptyDataFrame(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    val df = context.instanceRegistry.get[SnowflakeTableDataObject](dataObjectId).session.createDataFrame(Seq.empty[Row], inner)
    SnowparkDataFrame(df)
  }
  override def getDataType(colName: String): GenericDataType = SparkDataType(inner.apply(colName).dataType)
}

case class SnowparkColumn(inner: Column) extends GenericColumn {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def ===(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SnowparkColumn => SnowparkColumn(inner === sparkColumn.inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method ===")
    }
  }
  override def >(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SnowparkColumn => SnowparkColumn(inner > sparkColumn.inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method >")
    }
  }
  override def <(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SnowparkColumn => SnowparkColumn(inner < sparkColumn.inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method <")
    }
  }
  override def and(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SnowparkColumn => SnowparkColumn(inner and sparkColumn.inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method and")
    }
  }
  override def or(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SnowparkColumn => SnowparkColumn(inner or sparkColumn.inner)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method or")
    }
  }
  override def isin(list: Any*): GenericColumn = {
    // snowpark does not yet support isin operator
    SnowparkColumn(list.map(inner===_).reduce(_ or _))
  }
  override def isNull: GenericColumn = SnowparkColumn(inner.is_null)
  override def as(name: String): GenericColumn = SnowparkColumn(inner.as(name))
  override def cast(dataType: GenericDataType): GenericColumn = {
    dataType match {
      case sparkDataType: SparkDataType => SnowparkColumn(inner.cast(sparkDataType.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${subFeedType.typeSymbol.name} in method or")
    }
  }
  override def exprSql: String = inner.toString() // TODO: not sure if this is valid sql...
}

case class SparkDataType(inner: DataType) extends GenericDataType {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def isSortable: Boolean = Seq(StringType, LongType, IntegerType, ShortType, FloatType, DoubleType, TimestampType, DateType).contains(inner)
  override def typeName: String = inner.typeName
  override def sql: String = inner.typeName // TODO: not sure if this is valid sql...
}

case class SnowparkRow(inner: Row) extends GenericRow {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def get(index: Int): Any = inner.get(index)
  override def getAs[T](index: Int): T = get(index).asInstanceOf[T]
}