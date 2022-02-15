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

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table, TickTockHiveTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL

class HistorizeActionTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear()
  }

  test("historize 1st 2nd load") {

    val context = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val feed = "historize"
    val srcTable = Table(Some("default"), "historize_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable(context)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "historize_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable(context)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id)
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    {
      val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp)))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getDataFrame()(context1)
        .drop(Historization.historizeHashColName)
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("historize 1st load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id)
    val l2 = Seq(("doe","john",10)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())(context1)
    val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
    action2.exec(Seq(srcSubFeed2))(context2)

    {
      val expected = Seq(
        ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
        ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp))
      ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getDataFrame()(context1)
        .drop(Historization.historizeHashColName)
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("historize 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  test("early validation that output primary key exists") {
    // setup DataObjects
    val srcTable = Table(Some("default"), "historize_input")
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "historize_output")
    val tgtPath = tempPath+s"/${tgtTable.fullName}"
    val tgtDO = TickTockHiveTableDataObject( "tgt1", Some(tgtPath), table = tgtTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    intercept[IllegalArgumentException]{HistorizeAction("hist1", srcDO.id, tgtDO.id)}
  }
}
