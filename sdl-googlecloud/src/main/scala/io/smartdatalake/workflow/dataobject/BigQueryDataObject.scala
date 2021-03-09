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

package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BigQueryDataObject(override val id: DataObjectId,
                              override val metadata: Option[DataObjectMetadata] = None)
                             (@transient implicit val instanceRegistry: InstanceRegistry)
extends DataObject with CanCreateDataFrame {

  /**
   * Returns the factory that can parse this type (that is, type `CO`).
   *
   * Typically, implementations of this method should return the companion object of the implementing class.
   * The companion object in turn should implement [[FromConfigFactory]].
   *
   * @return the factory (object) for this class.
   */
  override def factory: FromConfigFactory[DataObject] = BigQueryDataObject

  override def getDataFrame(partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {

    session.read
      .format("bigquery")
      .load("bigquery-public-data.samples.shakespeare")

  }
}


object BigQueryDataObject extends FromConfigFactory[DataObject] {
  /**
   * Factory method for parsing (case) classes from [[Config]]s.
   *
   * @return a new instance of type `CO` parsed from the a context dependent [[Config]].
   */
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DataObject = {
    extract[BigQueryDataObject](config)
  }
}
