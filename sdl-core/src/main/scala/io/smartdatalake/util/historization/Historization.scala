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
package io.smartdatalake.util.historization

import java.time.LocalDateTime
import java.sql.Timestamp

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions
import io.smartdatalake.definitions.{HiveConventions, TechnicalTableColumn}
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.misc.{DataFrameUtil, SmartDataLakeLogger}
import io.smartdatalake.dataframe.{SDLColumn, SDLDataFrame, SDLStructField}


/**
 * Functions for historization
 */
private[smartdatalake] object Historization extends SmartDataLakeLogger {

  private[smartdatalake] val historizeHashColName = "dl_hash" // incrementalHistorize adds hash col to target schema for comparing changes
  private[smartdatalake] val historizeOperationColName = "dl_operation" // incrementalHistorize needs operation col for merge statement. It is temporary and is not added to target schema.

  // High value - symbolic value of timestamp with meaning of "without expiry date"
  //val doomsday = new java.sql.Timestamp(new java.util.Date("9999/12/31").getTime)
  private[smartdatalake] def doomsday(df: SDLDataFrame) = localDateTimeToCol(df, definitions.HiveConventions.getHistorizationSurrogateTimestamp)

  // "Tick" offset used to delimit timestamps of old and new values
  val offsetNs = 1000000L


  /**
   * Historizes data by merging the current load with the existing history
   *
   * Expects dfHistory and dfNew having the same schema. Use [[SchemaEvolution.process]] for preparation.
   *
   * @param dfHistory exsisting history of data
   * @param dfNew current load of feed
   * @param primaryKeyColumns Primary keys to join history with current load
   * @param historizeBlacklist optional list of columns to ignore when comparing two records. Can not be used together with historizeWhitelist.
   * @param historizeWhitelist optional final list of columns to use when comparing two records. Can not be used together with historizeBlacklist.
   * @return current feed merged with history
  */
  def fullHistorize(dfHistory: SDLDataFrame, dfNew: SDLDataFrame, primaryKeyColumns: Seq[String],
                    referenceTimestamp: LocalDateTime,
                    historizeWhitelist: Option[Seq[String]],
                    historizeBlacklist: Option[Seq[String]]
                   ): SDLDataFrame = {

    // Name for Hive column "last updated on ..."
    val lastUpdateCol = TechnicalTableColumn.captured

    // Name for Hive column "Replaced on ..."
    val expiryDateCol = TechnicalTableColumn.delimited

    // Current timestamp (used for insert and update operations, for "new" value)
    def timestampNew(df: SDLDataFrame) = localDateTimeToCol(df, referenceTimestamp)

    // Shortly before the current timestamp ("Tick") used for existing, old records
    def timestampOld(df: SDLDataFrame) = localDateTimeToCol(df, referenceTimestamp.minusNanos(offsetNs))

    // make sure history schema is equal to new feed schema
    val colsToIgnore = Seq(lastUpdateCol, expiryDateCol, "dl_dt")
    val structFields: Seq[SDLStructField] = dfHistory.schema.filterNot(n => colsToIgnore.contains(n.name))
    assert(SchemaEvolution.hasSameColNamesAndTypes(structFields, dfNew.schema.fields))

    // Records in history that still existed during the last execution
    val dfLastHist = dfHistory.where(dfHistory(expiryDateCol) === doomsday(dfHistory))

    // Records in history that already didn't exist during last execution
    val restHist = dfHistory.where(dfHistory(expiryDateCol) =!= doomsday(dfHistory))

    // add hash-column to easily compare changed records
    val colsToCompare = getCompareColumns(dfNew.columns, historizeWhitelist, historizeBlacklist)
    val dfNewHashed = dfNew.withColumn(historizeHashColName, colsComparisionExpr(dfNew, colsToCompare))
    val dfLastHistHashed = dfLastHist.withColumn(historizeHashColName, colsComparisionExpr(dfNew, colsToCompare))
    val hashColEqualExprLeftName = s"newFeed.$historizeHashColName"
    val hashColEqualExprRightName = s"lastHist.$historizeHashColName"

    val joined = dfNewHashed.as("newFeed")
      .join(dfLastHistHashed.as("lastHist"), joinCols(dfNewHashed, dfLastHistHashed, primaryKeyColumns), "full")

    val newRows = joined.where(joined(expiryDateCol).isNull)
      .select(dfNew("*"))
      .withColumn(lastUpdateCol, timestampNew(joined))
      .withColumn(expiryDateCol, doomsday(joined))

    val notInFeedAnymore = joined.where(nullTableCols(joined, "newFeed", primaryKeyColumns))
      .select(dfLastHist("*"))
      .withColumn(expiryDateCol, timestampOld(joined))

    val noUpdates = joined
      .where(joined(hashColEqualExprLeftName) === joined(hashColEqualExprRightName))
      .select(dfLastHist("*"))

    val updated = joined
      .where(nonNullTableCols(joined, "newFeed", primaryKeyColumns))
      .where(joined(hashColEqualExprLeftName) =!= joined(hashColEqualExprRightName))

    val updatedNew = updated.select(dfNew("*"))
      .withColumn(lastUpdateCol, timestampNew(updated))
      .withColumn(expiryDateCol, doomsday(updated))

    val updatedOld = updated.select(dfLastHist("*"))
      .withColumn(expiryDateCol, timestampOld(updated))

    // column order is used here!
    val dfNewHist = SchemaEvolution.sortColumns(notInFeedAnymore, dfHistory.columns)
      .union(SchemaEvolution.sortColumns(newRows, dfHistory.columns))
      .union(SchemaEvolution.sortColumns(updatedNew, dfHistory.columns))
      .union(SchemaEvolution.sortColumns(updatedOld, dfHistory.columns))
      .union(SchemaEvolution.sortColumns(noUpdates, dfHistory.columns))
      .union(SchemaEvolution.sortColumns(restHist, dfHistory.columns))

    if (logger.isDebugEnabled) {
      logger.debug(s"Count previous history: ${dfHistory.count}")
      logger.debug(s"Count current load of feed: ${dfNew.count}")
      logger.debug(s"Count rows not in current feed anymore: ${notInFeedAnymore.count}")
      logger.debug(s"Count new rows: ${newRows.count}")
      logger.debug(s"Count updated rows new: ${updatedNew.count}")
      logger.debug(s"Count updated rows old: ${updatedOld.count}")
      logger.debug(s"Count no updates old: ${noUpdates.count}")
      logger.debug(s"Count rows from remaining history: ${restHist.count}")
      logger.debug(s"Summary count rows new history: ${dfNewHist.count}")
    }

    dfNewHist
  }

  /**
   * Historizes data by merging the current load with the existing history, generating records to update and insert for a SQL Upsert Statement.
   *
   * SQL Upsert statement has great performance potential, but also its limitation:
   * - matched records can be updated or deleted
   * - unmatched records can be inserted
   *
   * Implementing historization with one SQL statement is not possible
   *  - update matched records (close version if column changed) -> supported
   *  - insert matched records (new version if columns changed) -> '''insert on match is not supported'''
   *  - insert unmatched records (new record) -> supported
   *  - update unmatched records in source (deleted record) -> '''not supported in SQL standard''' (MS SQL would have some extension with its MATCHED BY SOURCE/TARGET clause)
   *
   * This functions joins new data with existing current data and generates update and insert records for an SQL Upsert statement.
   * A full outer join between new and existing current data is made and the following records generated:
   *  1. primary key matched and attributes have changed -> update record to close existing version, insert record to create new version
   *  1. primary key unmatched, record only in new data -> insert record
   *  1. primary key unmatched, record only in existing data -> update record to close existing version
   *
   * Existing and new SDLDataFrame are not required to have the same schema, as schema evolution is handled by output DataObject.
   *
   * Compared with fullHistorized the following performance optimizations are implemented:
   *  - only current existing data needs to be read (delimited=doomsday)
   *  - only changed data needs to be written
   *  - a Column with hash-value calculated from all attributes is added to the target table, allowing to use only primary key and hashColumn for joining new data with existing data and detecting changes
   *
   *  Note that the use of hashColumn to detect changed records will create new version for every record on schema evolution.
   *  This behaviour is different from fullHistorize.
   */
  def incrementalHistorize(dfExisting: SDLDataFrame,
                           dfNew: SDLDataFrame,
                           primaryKey: Seq[String],
                           referenceTimestamp: LocalDateTime,
                           historizeWhitelist: Option[Seq[String]],
                           historizeBlacklist: Option[Seq[String]]) : SDLDataFrame = {
    // Current timestamp (used for insert and update operations, for "new" value)
    def timestampNew(df: SDLDataFrame) = localDateTimeToCol(df, referenceTimestamp)
    // Shortly before the current timestamp ("Tick") used for existing, old records
    def timestampOld(df: SDLDataFrame) = localDateTimeToCol(df, referenceTimestamp.minusNanos(offsetNs))
    // prepare columns
    val existingCapturedCol = dfExisting(s"existing.${TechnicalTableColumn.captured}")
    val existingDelimitedCol = dfExisting(s"existing.${TechnicalTableColumn.delimited}")
    val existingHashCol = dfExisting(s"existing.$historizeHashColName")
    val newHashCol = dfNew(s"new.$historizeHashColName")
    val hashColEqualsExpr = existingHashCol === newHashCol
    // add hash column
    val dfNewHashed = addHashCol(dfNew, historizeWhitelist, historizeBlacklist, useHash = true)
    // join existing with new and determine operations needed
    val dfOperations = dfExisting.as("existing")
      .where(existingDelimitedCol === doomsday(dfExisting)) // only current records needed
      .select((primaryKey :+ TechnicalTableColumn.captured :+ historizeHashColName).toArray)
      .join(dfNewHashed.as("new"), primaryKey.map(colName => dfExisting(colName)), "full")
      .withColumn("_operations",
         // 1. primary key matched and attributes have changed -> update record to close existing version, insert record to create new version
         dfExisting.when(existingHashCol.isNotNull and newHashCol.isNotNull and !hashColEqualsExpr,
           dfExisting.array(dfExisting.lit(HistorizationRecordOperations.updateClose), dfExisting.lit(dfExisting.lit(HistorizationRecordOperations.insertNew))))
         // 2. record only in new data -> insert new record
        .when(existingHashCol.isNull and newHashCol.isNotNull,
           dfExisting.array(dfExisting.lit(HistorizationRecordOperations.insertNew)))
         // 3. record only in existing data -> update record to close existing version
        .when(existingHashCol.isNotNull and newHashCol.isNull,
           dfExisting.array(dfExisting.lit(HistorizationRecordOperations.updateClose)))
      )
    // add versioning data
    val dfOperationVersioned = dfOperations
      .explode(dfOperations("_operations"), historizeOperationColName, outer = false) // note: this filters records with no action
      .drop(dfOperations("_operations"))
      .drop(existingHashCol)
      .withColumn(TechnicalTableColumn.captured,
         dfOperations.when(dfOperations(historizeOperationColName) === HistorizationRecordOperations.insertNew, timestampNew(dfOperations))
        .when(dfOperations(historizeOperationColName) === HistorizationRecordOperations.updateClose, existingCapturedCol)
      )
      .withColumn(TechnicalTableColumn.delimited,
         dfOperations.when(dfOperations(historizeOperationColName) === HistorizationRecordOperations.insertNew, doomsday(dfOperations))
        .when(dfOperations(historizeOperationColName) === HistorizationRecordOperations.updateClose, timestampOld(dfOperations))
      )
      .drop(existingCapturedCol)
    // return
    dfOperationVersioned
  }

  /**
   * Creates initial history of feed for fullHistorization
   *
   * @param df current run of feed
   * @param referenceTimestamp timestamp to use
   * @return initial history, identical with data from current run
   */
  def getFullInitialHistory(df: SDLDataFrame, referenceTimestamp: LocalDateTime): SDLDataFrame = {
    logger.debug(s"Initial history used for ${TechnicalTableColumn.captured}: $referenceTimestamp")
    addVersionCols(df, referenceTimestamp, HiveConventions.getHistorizationSurrogateTimestamp)
  }

  /**
   * Creates initial history of feed for incrementalHistorization
   *
   * @param df current run of feed
   * @param referenceTimestamp timestamp to use
   * @return initial history, identical with data from current run
   */
  def getIncrementalInitialHistory(df: SDLDataFrame, referenceTimestamp: LocalDateTime, historizeWhitelist: Option[Seq[String]], historizeBlacklist: Option[Seq[String]]): SDLDataFrame = {
    logger.debug(s"Initial history used for ${TechnicalTableColumn.captured}: $referenceTimestamp")
    val df1 = addHashCol(df, historizeWhitelist, historizeBlacklist, useHash = true)
    addVersionCols(df1, referenceTimestamp, HiveConventions.getHistorizationSurrogateTimestamp)
      .withColumn(historizeOperationColName, df.lit(HistorizationRecordOperations.insertNew))
  }

  private[smartdatalake] def addVersionCols(df: SDLDataFrame, captured: LocalDateTime, delimited: LocalDateTime): SDLDataFrame = {
    df.withColumn(TechnicalTableColumn.captured, localDateTimeToCol(df, captured))
      .withColumn(TechnicalTableColumn.delimited, localDateTimeToCol(df, delimited))
  }

  private def joinCols(left: SDLDataFrame, right: SDLDataFrame, cols: Seq[String]): SDLColumn = {
    cols.map(c => left(c) === right(c)).reduce(_ and _)
  }

  private def nullTableCols(df: SDLDataFrame, table: String, cols: Seq[String]): SDLColumn = {
    cols.map(c => df(s"$table.$c").isNull).reduce(_ and _)
  }

  private def nonNullTableCols(df: SDLDataFrame, table: String, cols: Seq[String]): SDLColumn = {
    cols.map(c => df(s"$table.$c").isNotNull).reduce(_ and _)
  }

  private[smartdatalake] def localDateTimeToTstmp(dateTime: LocalDateTime): Timestamp = Timestamp.valueOf(dateTime)
  private[smartdatalake] def localDateTimeToCol(df: SDLDataFrame, dateTime: LocalDateTime): SDLColumn = df.lit(Timestamp.valueOf(dateTime))

  private[smartdatalake] def getCompareColumns(colsToUse: Seq[String], historizeWhitelist: Option[Seq[String]], historizeBlacklist: Option[Seq[String]]): Seq[String] = {
    val colsToCompare = (historizeWhitelist, historizeBlacklist) match {
      case (Some(w), None) => colsToUse.intersect(w) // merged columns from whitelist und dfLastHist without technical columns
      case (None, Some(b)) => colsToUse.diff(b)
      case (None, None) => colsToUse
      case (Some(_), Some(_)) => throw new ConfigurationException("historize-whitelist and historize-blacklist must not be used at the same time.")
    }
    colsToCompare.toSeq.sorted
  }

  // Generic column expression to compare a list of columns
  private[smartdatalake] def colsComparisionExpr(df: SDLDataFrame, cols: Seq[String], useHash: Boolean = false): SDLColumn = {
    logger.debug(s"using hash columns ${cols.sorted.mkString(",")}")
    df.struct(cols.sorted.map(colName => df(colName)): _*)
  }

  private[smartdatalake] def addHashCol(df: SDLDataFrame, historizeWhitelist: Option[Seq[String]], historizeBlacklist: Option[Seq[String]], useHash: Boolean, colsToIgnore: Seq[String] = Seq()): SDLDataFrame = {
    val colsToCompare = getCompareColumns(df.columns.diff(colsToIgnore), historizeWhitelist, historizeBlacklist)
    df.withColumn(historizeHashColName, colsComparisionExpr(df, colsToCompare, useHash))
  }
}

private[smartdatalake] object HistorizationRecordOperations {
  val updateClose = "updateClose"
  val insertNew = "insertNew"
}
