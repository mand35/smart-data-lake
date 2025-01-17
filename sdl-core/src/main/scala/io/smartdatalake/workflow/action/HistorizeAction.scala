/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.{Historization, HistorizationRecordOperations}
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef, SparkDfTransformerFunctionWrapper}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanMergeDataFrame, DataObject, TransactionalSparkTableDataObject}
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.reflect.runtime.universe.Type
import scala.util.{Failure, Success, Try}

/**
 * [[Action]] to historize a subfeed.
 * Historization creates a technical history of data by creating valid-from/to columns.
 * It needs a transactional table as output with defined primary keys.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param filterClause Filter of data to be processed by historization. It can be used to exclude historical data not needed to create new history, for performance reasons.
 *                     Note that filterClause is only applied if mergeModeEnable=false. Use mergeModeAdditionalJoinPredicate if mergeModeEnable=true to achieve a similar performance tuning.
 * @param historizeBlacklist optional list of columns to ignore when comparing two records in historization. Can not be used together with [[historizeWhitelist]].
 * @param historizeWhitelist optional final list of columns to use when comparing two records in historization. Can not be used together with [[historizeBlacklist]].
 * @param ignoreOldDeletedColumns if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns from nested data types in Schema Evolution.
 *                                      Keeping deleted columns in complex data types has performance impact as all new data
 *                                      in the future has to be converted by a complex function.
 * @param transformer optional custom transformation to apply
 * @param transformers optional list of transformations to apply before historization. See [[sparktransformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
 * @param mergeModeEnable Set to true to use saveMode.Merge for much better performance. Output DataObject must implement [[CanMergeDataFrame]] if enabled (default = false).
 * @param mergeModeAdditionalJoinPredicate To optimize performance it might be interesting to limit the records read from the existing table data, e.g. it might be sufficient to use only the last 7 days.
 *                                Specify a condition to select existing data to be used in transformation as Spark SQL expression.
 *                                Use table alias 'existing' to reference columns of the existing table data.
 * @param executionMode optional execution mode for this Action
 * @param executionCondition optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class HistorizeAction(
                            override val id: ActionId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            @Deprecated @deprecated("Use transformers instead.", "2.0.5")
                            transformer: Option[CustomDfTransformerConfig] = None,
                            transformers: Seq[GenericDfTransformer] = Seq(),
                            filterClause: Option[String] = None,
                            historizeBlacklist: Option[Seq[String]] = None,
                            historizeWhitelist: Option[Seq[String]] = None,
                            ignoreOldDeletedColumns: Boolean = false,
                            ignoreOldDeletedNestedColumns: Boolean = true,
                            mergeModeEnable: Boolean = false,
                            mergeModeAdditionalJoinPredicate: Option[String] = None,
                            override val breakDataFrameLineage: Boolean = false,
                            override val persist: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val executionCondition: Option[Condition] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None
                          )(implicit instanceRegistry: InstanceRegistry) extends DataFrameOneToOneActionImpl {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: TransactionalSparkTableDataObject = getOutputDataObject[TransactionalSparkTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalSparkTableDataObject] = Seq(output)

  private val mergeModeAdditionalJoinPredicateExpr: Option[Column] = try {
    mergeModeAdditionalJoinPredicate.map(expr)
  } catch {
    case ex: Exception => throw new ConfigurationException(s"($id) Cannot parse mergeModeAdditionalJoinPredicate as Spark expression: ${ex.getClass.getSimpleName} ${ex.getMessage}", Some(s"{$id.id}.mergeModeAdditionalJoinPredicate"), ex)
  }
  if (!mergeModeEnable && mergeModeAdditionalJoinPredicateExpr.nonEmpty) logger.warn(s"($id) Configuration of mergeModeAdditionalJoinPredicate has no effect if mergeModeEnable = false")

  override def saveModeOptions: Option[SaveModeOptions] = if (mergeModeEnable) {
    // force SDLSaveMode.Merge if mergeModeEnable = true
    assert(output.isInstanceOf[CanMergeDataFrame], s"($id) output DataObject must support SaveMode.Merge (implement CanMergeDataFrame) if mergeModeEnable = true")
    // customize update condition
    val updateCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateClose}'")
    val updateCols = Seq(TechnicalTableColumn.delimited)
    val insertCondition =  Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
    val insertColsToIgnore = Seq(Historization.historizeOperationColName)
    val additionalMergePredicate = Some((s"new.${TechnicalTableColumn.captured} = existing.${TechnicalTableColumn.captured}" +: mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
    Some(SaveModeMergeOptions(updateCondition = updateCondition, updateColumns = updateCols, insertCondition = insertCondition, insertColumnsToIgnore = insertColsToIgnore, additionalMergePredicate = additionalMergePredicate))
  } else {
    // force SDLSaveMode.Overwrite otherwise
    Some(SaveModeGenericOptions(SDLSaveMode.Overwrite))
  }

  // Output is used as recursive input in DeduplicateAction to get existing data. This override is needed to force tick-tock write operation.
  override val recursiveInputs: Seq[TransactionalSparkTableDataObject] = Seq(output)

  private[smartdatalake] override val handleRecursiveInputsAsSubFeeds: Boolean = false

  // historize black/white list
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty, s"(${id}) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // primary key
  require(output.table.primaryKey.isDefined, s"(${id}) Primary key must be defined for output DataObject")

  private val transformerDefs: Seq[GenericDfTransformerDef] = transformer.map(t => t.impl).toSeq ++ transformers

  override val transformerSubFeedSupportedTypes: Seq[Type] = transformerDefs.map(_.getSubFeedSupportedType) // historize transformer can be ignored as it is generic

  validateConfig()

  private def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val capturedTs = context.referenceTimestamp.getOrElse(LocalDateTime.now)
    val pks = output.table.primaryKey.get // existance is validated earlier

    // get existing data
    // Note that HistorizeAction with mergeModeEnabled=false needs to read/write all existing data for tick-tock operation, even if only specific partitions have changed
    val existingDf = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType)) else None

    // historize
    val historizeTransformer = if (mergeModeEnable) {
      // TODO: make generic
      val historizeFunction = incrementalHistorizeDataFrame(existingDf.map(_.asInstanceOf[SparkDataFrame].inner), pks, capturedTs) _
      SparkDfTransformerFunctionWrapper("incrementalHistorize", historizeFunction)
    } else {
      // TODO: make generic
      val historizeFunction = fullHistorizeDataFrame(existingDf.map(_.asInstanceOf[SparkDataFrame].inner), pks, capturedTs) _
      SparkDfTransformerFunctionWrapper("fullHistorize", historizeFunction)
    }
    transformerDefs :+ historizeTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    applyTransformers(getTransformers, partitionValues)
  }

  // TODO: make generic
  protected def fullHistorizeDataFrame(existingDf: Option[DataFrame], pks: Seq[String], refTimestamp: LocalDateTime)(newDf: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession

    // parse filter clause
    val filterClauseExpr = Try(filterClause.map(expr)) match {
      case Success(result) => result
      case Failure(e) => throw new ConfigurationException(s"(${id}) Error parsing filterClause parameter as expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    val newFeedDf = newDf.dropDuplicates(pks)

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get.where(filterClauseExpr.getOrElse(lit(true))), TechnicalTableColumn.captured)
      // apply schema evolution
      val (modifiedExistingDf, modifiedNewFeedDf) = SchemaEvolution.process(existingDf.get, newFeedDf, ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns
        , colsToIgnore = Seq(TechnicalTableColumn.captured, TechnicalTableColumn.delimited))
      // filter existing data to be excluded from historize operation
      val (filteredExistingDf, filteredExistingRemainingDf) =
        filterClauseExpr match {
          case Some(expr) => (modifiedExistingDf.where(expr), Some(modifiedExistingDf.where(not(expr))))
          case None => (modifiedExistingDf, None)
        }
      // historize
      val historizedDf = Historization.fullHistorize(filteredExistingDf, modifiedNewFeedDf, pks, refTimestamp, historizeWhitelist, historizeBlacklist)
      // union with filter remaining df and return
      if (filteredExistingRemainingDf.isDefined) historizedDf.union(filteredExistingRemainingDf.get)
      else historizedDf
    } else Historization.getFullInitialHistory(newFeedDf, refTimestamp)
  }

  // TODO: make generic
  protected def incrementalHistorizeDataFrame(existingDf: Option[DataFrame], pks: Seq[String], refTimestamp: LocalDateTime)(newDf: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession

    val newFeedDf = newDf.dropDuplicates(pks)

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, TechnicalTableColumn.captured)
      // historize
      // note that schema evolution is done by output DataObject
      Historization.incrementalHistorize(existingDf.get, newDf, pks, refTimestamp, historizeWhitelist, historizeBlacklist)
    } else Historization.getIncrementalInitialHistory(newFeedDf, refTimestamp, historizeWhitelist, historizeBlacklist)
  }

  override def factory: FromConfigFactory[Action] = HistorizeAction
}

object HistorizeAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HistorizeAction = {
    extract[HistorizeAction](config)
  }
}