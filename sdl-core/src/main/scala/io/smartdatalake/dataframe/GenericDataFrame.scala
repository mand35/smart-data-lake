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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}

import scala.collection.mutable
import scala.reflect.runtime.universe.Type


trait GenericDataFrame {
  def subFeedType: Type

  def schema: GenericSchema

  def join(other: GenericDataFrame, joinCols: Seq[String]): GenericDataFrame

  def select(columns: Seq[GenericColumn]): GenericDataFrame

  def agg(columns: Seq[GenericColumn]): GenericDataFrame

  def unionByName(other: GenericDataFrame): GenericDataFrame

  def filter(expression: GenericColumn): GenericDataFrame

  def collect: Seq[GenericRow]

  def withColumn(colName: String, expression: GenericColumn): GenericDataFrame

  def createOrReplaceTempView(viewName: String): Unit

  /**
   * Move partition columns at end of DataFrame as required when writing to Hive in Spark > 2.x
   */
  def movePartitionColsLast(partitions: Seq[String])(implicit helper: DataFrameSubFeedCompanion): GenericDataFrame = {
    val (partitionCols, nonPartitionCols) = schema.columns.partition(c => partitions.contains(c))
    val newColOrder = nonPartitionCols ++ partitionCols
    select(newColOrder.map(helper.col))
  }

  /**
   * Convert column names to lower case
   */
  def colNamesLowercase(implicit helper: DataFrameSubFeedCompanion): GenericDataFrame = {
    select(schema.columns.map(c => helper.col(c).as(c.toLowerCase())))
  }

  def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed
}
trait GenericSchema {
  def subFeedType: Type
  def convertIfNeeded(toSubFeedType: Type): GenericSchema = {
    if (this.subFeedType != toSubFeedType) SchemaConverter.convert(this, toSubFeedType)
    else this
  }
  def diffSchema(schema: GenericSchema): Option[GenericSchema]
  def columns: Seq[String]
  def sql: String
  def add(colName: String, dataType: GenericDataType): GenericSchema
  def getEmptyDataFrame(implicit context: ActionPipelineContext): GenericDataFrame
  def getDataType(colName: String): GenericDataType
}
trait GenericColumn {
  def subFeedType: Type
  def ===(other: GenericColumn): GenericColumn
  def and(other: GenericColumn): GenericColumn
  def or(other: GenericColumn): GenericColumn
  @scala.annotation.varargs
  def isin(list: Any*): GenericColumn
  def as(name: String): GenericColumn
  def cast(dataType: GenericDataType): GenericColumn
}
trait GenericDataType {
  def subFeedType: Type
  def convertIfNeeded(toSubFeedType: Type): GenericDataType = {
    if (this.subFeedType != toSubFeedType) SchemaConverter.convertDataType(this, toSubFeedType)
    else this
  }
  def isSortable: Boolean
  def typeName: String
  def sql: String
}
trait GenericRow {
  def get(index: Int): Any
  def getAs[T](index: Int): T
}

/*
object DataFrameSubFeedHelper {
  // search all classes implementing DataFrameSubFeedHelper
  private lazy val helpersCache = mutable.Map[Type, DataFrameSubFeedHelper]()

  /**
   * Get the DataFrameSubFeedHelper for a given SubFeed type
   */
  def apply(subFeedType: Type): DataFrameSubFeedHelper = {
    helpersCache.getOrElseUpdate(subFeedType, {
      val mirror = scala.reflect.runtime.currentMirror
      try {
        val module = mirror.staticModule(subFeedType.typeSymbol.name.toString)
        mirror.reflectModule(module).instance.asInstanceOf[DataFrameSubFeedHelper]
      } catch {
        case e: Exception => throw new IllegalStateException(s"No DataFrameSubFeedHelper implementation found for SubFeed type ${subFeedType.typeSymbol.name}", e)
      }
    })
  }
}
*/

trait SchemaConverter {
  def fromSubFeedType: Type
  def toSubFeedType: Type
  def convert(schema: GenericSchema): GenericSchema
  def convertDataType(dataType: GenericDataType): GenericDataType
}
object SchemaConverter {
  private val converters: mutable.Map[(Type,Type), SchemaConverter] = mutable.Map()
  def registerConverter(converter: SchemaConverter): Unit = {
    converters.put(
      (converter.fromSubFeedType,converter.toSubFeedType),
      converter
    )
  }
  def convert(schema: GenericSchema, toSubFeedType: Type): GenericSchema = {
    val converter = converters.get(schema.subFeedType, toSubFeedType)
      .getOrElse(throw new IllegalStateException(s"No schema converter found from ${schema.subFeedType.typeSymbol.name} to ${toSubFeedType.typeSymbol.name}"))
    converter.convert(schema)
  }
  def convertDataType(dataType: GenericDataType, toSubFeedType: Type): GenericDataType = {
    val converter = converters.get(dataType.subFeedType, toSubFeedType)
      .getOrElse(throw new IllegalStateException(s"No SchemaConverter found from ${dataType.subFeedType.typeSymbol.name} to ${toSubFeedType.typeSymbol.name}"))
    converter.convertDataType(dataType)
  }
}
