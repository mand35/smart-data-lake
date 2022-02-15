/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.dataframe.GenericSchema
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.{sql => spark}
import com.snowflake.snowpark
import io.smartdatalake.dataframe.snowflake.SnowparkSubFeed

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * [[DataObject]] of type SnowflakeTableDataObject.
 * Provides details to access Snowflake tables via an action
 * Can be used both for interacting with Snowflake through Spark with JDBC,
 * as well as for actions written in the Snowpark API that run directly on Snowflake
 *
 * @param id           unique name of this data object
 * @param table        Snowflake table to be written by this output
 * @param saveMode     spark [[SDLSaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId The SnowflakeTableConnection to use for the table
 * @param comment      An optional comment to add to the table after writing a DataFrame to it
 * @param metadata     meta data
 */
// TODO: we should add virtual partitions as for JdbcTableDataObject and KafkaDataObject, so that PartitionDiffMode can be used...
case class SnowflakeTableDataObject(override val id: DataObjectId,
                                    override var table: Table,
                                    override val schemaMin: Option[GenericSchema] = None,
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    connectionId: ConnectionId,
                                    comment: Option[String] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject {

  private val connection = getConnection[SnowflakeConnection](connectionId)

  def session: snowpark.Session = {
    connection.getSnowparkSession(table.db.get)
  }

  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) A SnowFlake schema name must be added as the 'db' parameter of a SnowflakeTableDataObject.")
  }

  // Get a Spark DataFrame with the table contents for Spark transformations
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): spark.DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query", q)).getOrElse("dbtable" -> (connection.database + "." + table.fullName)))
    val df = context.sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(queryOrTable)
      .load()
    df.colNamesLowercase
  }

  // Write a Spark DataFrame to the Snowflake table
  override def writeSparkDataFrame(df: spark.DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): Unit = {
    validateSchemaMin(SparkSchema(df.schema), role = "write")
    val finalSaveMode: SDLSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(Map("dbtable" -> (connection.database + "." + table.fullName)))
      .mode(finalSaveMode.asSparkSaveMode)
      .save()

    if (comment.isDefined) {
      val sql = s"comment on table ${connection.database}.${table.fullName} is '$comment';"
      connection.execSnowflakeStatement(sql)
    }
  }

  private[smartdatalake] override def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Class[_])(implicit context: ActionPipelineContext): GenericDataFrameSubFeed = {
    subFeedType match {
      case t if t == classOf[SparkSubFeed] => SparkSubFeed(Some(SparkDataFrame(getSparkDataFrame(partitionValues))), id, partitionValues)
      case t if t == classOf[SnowparkSubFeed] => SnowparkSubFeed(Some(getSnowparkDataFrame()), id, partitionValues)
    }
  }
  private[smartdatalake] override def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkSubFeed], typeOf[SnowparkSubFeed])

  private[smartdatalake] override def writeSubFeed(subFeed: DataFrameSubFeed, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = {
    subFeed match {
      case sparkSubFeed: SparkSubFeed => writeDataFrame(sparkSubFeed.dataFrame.get, partitionValues, isRecursiveInput, saveModeOptions)
      case snowparkSubFeed: SnowparkSubFeed => writeSnowparkDataFrame(snowparkSubFeed.dataFrame.get, isRecursiveInput, saveModeOptions)
    }
  }
  private[smartdatalake] override def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkSubFeed], typeOf[SnowparkSubFeed])

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW DATABASES LIKE '${connection.database}'"
    connection.execSnowflakeStatement(sql).next()
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW TABLES LIKE '${table.name}' IN SCHEMA ${connection.database}.${table.db.get}"
    connection.execSnowflakeStatement(sql).next()
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = throw new NotImplementedError()

  override def factory: FromConfigFactory[DataObject] = SnowflakeTableDataObject

  /**
   * Read the contents of a table as a Snowpark DataFrame
   */
  def getSnowparkDataFrame()(implicit context: ActionPipelineContext): snowpark.DataFrame = {
    this.session.table(table.fullName)
  }

  /**
   * Write a Snowpark DataFrame to Snowflake, used in Snowpark actions
   */
  def writeSnowparkDataFrame(df: snowpark.DataFrame, isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                            (implicit context: ActionPipelineContext): Unit = {
    // TODO: implement saveMode & isRecursiveInput...
    df.write.saveAsTable(table.fullName)
  }
}

object SnowflakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)
                         (implicit instanceRegistry: InstanceRegistry): SnowflakeTableDataObject = {
    extract[SnowflakeTableDataObject](config)
  }
}