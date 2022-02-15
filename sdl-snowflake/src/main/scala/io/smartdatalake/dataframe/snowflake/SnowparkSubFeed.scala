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

import com.snowflake.snowpark.Row
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.dataframe.GenericDataFrame
import io.smartdatalake.definitions.ExecutionModeResult
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, SubFeed, SubFeedConverter}

import scala.reflect.runtime.universe.{Type, typeOf}

case class SnowparkSubFeed(@transient override val dataFrame: Option[SnowparkDataFrame],
                           override val dataObjectId: DataObjectId,
                           override val partitionValues: Seq[PartitionValues],
                           override val isDAGStart: Boolean = false,
                           override val isSkipped: Boolean = false,
                           override val isDummy: Boolean = false,
                           override val filter: Option[String] = None
                          )
  extends DataFrameSubFeed {
  override val tpe: Type = typeOf[SnowparkSubFeed]

  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy(partitionValues = Seq())
  }

  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    this.copy(partitionValues = updatedPartitionValues)
  }

  override def clearDAGStart(): SnowparkSubFeed = {
    this.copy(isDAGStart = false)
  }

  override def clearSkipped(): SnowparkSubFeed = {
    this.copy(isSkipped = false)
  }

  override def toOutput(dataObjectId: DataObjectId): SnowparkSubFeed = {
    this.copy(dataFrame = None, filter = None, isDAGStart = false, isSkipped = false, isDummy = false, dataObjectId = dataObjectId)
  }

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case snowparkSubFeed: SnowparkSubFeed if this.dataFrame.isDefined && snowparkSubFeed.dataFrame.isDefined =>
      this.copy(dataFrame = Some(this.dataFrame.get.unionByName(snowparkSubFeed.dataFrame.get)),
        partitionValues = unionPartitionValues(snowparkSubFeed.partitionValues),
        isDAGStart = this.isDAGStart || snowparkSubFeed.isDAGStart)
    case subFeed =>
      this.copy(dataFrame = None,
        partitionValues = unionPartitionValues(subFeed.partitionValues),
        isDAGStart = this.isDAGStart || subFeed.isDAGStart)
  }

  override def clearFilter(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    // if filter is removed, normally also the DataFrame must be removed so that the next action get's a fresh unfiltered DataFrame with all data of this DataObject
    if (breakLineageOnChange && filter.isDefined) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearFilter")
      this.copy(filter = None).breakLineage
    } else this.copy(filter = None)
  }

  override def persist: SnowparkSubFeed = {
    logger.warn("Persist is not implemented by Snowpark")
    // TODO: should we use "dataFrame.map(_.inner.cacheResult())"
    this
  }

  override def breakLineage(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    // in order to keep the schema but truncate logical plan, a dummy DataFrame is created.
    // dummy DataFrames must be exchanged to real DataFrames before reading in exec-phase.
    if(dataFrame.isDefined && !isDummy && !context.simulation) convertToDummy(dataFrame.get.schema) else this
  }

  override def hasReusableDataFrame: Boolean = dataFrame.isDefined && !isDummy && !isStreaming.getOrElse(false)

  private[smartdatalake] def convertToDummy(schema: SnowparkSchema)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    val dummyDf = dataFrame.map(_ => schema.getEmptyDataFrame(dataObjectId))
    this.copy(dataFrame = dummyDf, isDummy = true)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    // apply input filter
    val inputFilter = if (this.dataObjectId == mainInputId) result.filter else None
    this.copy(partitionValues = result.inputPartitionValues, filter = inputFilter, isSkipped = false).breakLineage // breaklineage keeps DataFrame schema without content
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, filter = result.filter, isSkipped = false, dataFrame = None)
  }
  override def withDataFrame(dataFrame: Option[GenericDataFrame]): SnowparkSubFeed = this.copy(dataFrame = dataFrame.map(_.asInstanceOf[SnowparkDataFrame]))
  override def withPartitionValues(partitionValues: Seq[PartitionValues]): DataFrameSubFeed = this.copy(partitionValues = partitionValues)
  override def asDummy(): SnowparkSubFeed = this.copy(isDummy = true)
  override def withFilter(partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = {
    this.copy(partitionValues = partitionValues, filter = filter)
      .applyFilter
  }
}

object SnowparkSubFeed extends SubFeedConverter[SnowparkSubFeed] {
  override def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    subFeed match {
      case snowparkSubFeed: SnowparkSubFeed => snowparkSubFeed
      case _ => SnowparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}