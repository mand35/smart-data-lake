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
package io.smartdatalake.util.evolution

import io.smartdatalake.dataframe.{SDLColumn, SDLDataFrame, SDLDataType, SDLStructField, SDLStructType, SparkArrayType, SparkMapType, SparkSDLStructType, SparkStructType}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger

import scala.util.Try


/**
 * Functions for schema evolution
 */
private[smartdatalake] object SchemaEvolution extends SmartDataLakeLogger {

  /**
   * Sorts all columns of a [[SDLDataFrame]] according to defined sort order
   *
   * @param df
   * @param cols
   * @return
   */
  def sortColumns(df: SDLDataFrame, cols: Seq[String]): SDLDataFrame = {
    val dfCols = df.columns
    val colsToSelect = cols.filter(c => dfCols.contains(c))
    df.select(colsToSelect.toArray)
  }

  def getFieldTuples(dataFrame: SDLDataFrame): Set[(String, String)] = {
    getFieldTuples(dataFrame.schema)
  }

  def getFieldTuples(schema: Seq[SDLStructField]): Set[(String, String)] = {
    schema.map(f => (f.name, f.dataType.simpleString)).toSet
  }

  /**
   * Checks if a schema evolution is necessary and if yes creates the evolved [[SDLDataFrame]]s.
   *
   * The following schema changes are supported
   * - Deleted columns: newDf contains less columns than oldDf and the remaining are identical
   * - New columns: newDf contains additional columns, all other columns are the same as in in oldDf
   * - Renamed columns: this is a combination of a deleted column and a new column
   * - Changed data type: see method [[convertSDLDataType]] for allowed changes of data type. In case of unsupported changes
   * of data types a [[SchemaEvolutionException]] is thrown
   *
   * @param oldDf                         [[SDLDataFrame]] with old data
   * @param newDf                         [[SDLDataFrame]] with new data with potential changes in schema
   * @param colsToIgnore                  technical columns to be ignored in oldDf (e.g TechnicalTableColumn.captured and TechnicalTableColumn.delimited for historization)
   * @param ignoreOldDeletedColumns       if true, remove no longer existing columns in result SDLDataFrame's
   * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns in result SDLDataFrame's. Keeping deleted
   *                                      columns in complex data types has performance impact as all new data in the future
   *                                      has to be converted by a complex function.
   * @return tuple of (oldExtendedDf, newExtendedDf) evolved to new schema
   */
  def process(oldDf: SDLDataFrame, newDf: SDLDataFrame, colsToIgnore: Seq[String] = Seq(), ignoreOldDeletedColumns: Boolean = false, ignoreOldDeletedNestedColumns: Boolean = true): (SDLDataFrame, SDLDataFrame) = {
    // internal structure and functions
    case class ColumnDetail(name: String, oldToNewColumn: Option[SDLColumn], newColumn: Option[SDLColumn], infoMsg: Option[String], errMsg: Option[String])
    def getNullColumnOfType(df: SDLDataFrame, d: SDLDataType) = df.lit(null).cast(d)

    // log entry point
    logger.debug(s"old schema: ${oldDf.schema.treeString}")
    logger.debug(s"new schema: ${newDf.schema.treeString}")

    val oldColsWithoutTechCols = oldDf.columns.filter(c => !colsToIgnore.contains(c)).toSeq
    val newColsWithoutTechCols = newDf.columns.filter(c => !colsToIgnore.contains(c)).toSeq

    // check if schema is identical
    if (hasSameColNamesAndTypes(oldDf.select(oldColsWithoutTechCols.toArray), newDf.select(newColsWithoutTechCols.toArray))) {
      // check column order
      if (oldColsWithoutTechCols == newColsWithoutTechCols) {
        logger.info("Schemas are identical: no evolution needed")
        (oldDf, newDf)
      } else {
        logger.info("Schemas are identical but column order differs: columns of newDf are sorted according to oldDf")
        val newSchemaOnlyCols = newDf.columns.diff(oldColsWithoutTechCols)
        (oldDf, newDf.select((oldColsWithoutTechCols ++ newSchemaOnlyCols).toArray))
      }
    } else {

      // prepare target column names
      // this defines the ordering of the resulting SDLDataFrame's
      val tgtCols = if (Environment.schemaEvolutionNewColumnsLast) {
        // new columns last
        oldColsWithoutTechCols ++ newColumns(oldDf, newDf) ++ colsToIgnore
      } else {
        // deleted columns last
        newColsWithoutTechCols ++ deletedColumns(oldDf, newDf) ++ colsToIgnore
      }

      // create mapping
      val tgtColumns = tgtCols.map {
        c =>
          val oldType = oldDf.schema.fields.find(_.name == c).map(_.dataType)
          val newType = newDf.schema.fields.find(_.name == c).map(_.dataType)
          val thisColumn = Some(oldDf(c))
          // define conversion
          val (oldToNewColumn, newColumn, infoMsg, errMsg) = (oldType, newType) match {
            // column is new -> fill in old data with null
            case (None, Some(n)) =>
              val nullColumn = Some(getNullColumnOfType(oldDf, n).as(c))
              val info = Some(s"column $c is new")
              (nullColumn, thisColumn, info, None)
            // column is old -> fill in new data with null
            case (Some(o), None) =>
              val (oldToNewColumn, newColumn, info) = if (colsToIgnore.contains(c)) (thisColumn, None, Some(s"column $c is ignored because it is in the list of columns to ignore"))
              else if (ignoreOldDeletedColumns) (None, None, Some(s"column $c is old and will be removed because ignoreOldDeletedColumns=true"))
              else (thisColumn, Some(getNullColumnOfType(oldDf, o).as(c)), Some(s"column $c is old and will be set to null for new records"))
              (oldToNewColumn, newColumn, info, None)
            // datatypes are *not* equal -> conversion of old to new datatype required
            case (Some(o), Some(n)) if o.simpleString != n.simpleString =>
              val convertedColumns = convertSDLDataType(oldDf(c), o, n, ignoreOldDeletedNestedColumns)
              val info = if (convertedColumns.isDefined) Some(s"column $c is converted from ${o.simpleString}/${n.simpleString} to ${convertedColumns.get._3.simpleString}") else None
              val err = if (convertedColumns.isEmpty) Some(s"column $c cannot be converted from ${o.simpleString} to ${n.simpleString}") else None
              (convertedColumns.map(_._1.as(c)), convertedColumns.map(_._2.as(c)), info, err)
            // datatypes are equal -> no conversion required
            case (Some(o), Some(n)) => (thisColumn, thisColumn, None, None)
          }
          ColumnDetail(c, oldToNewColumn, newColumn, infoMsg, errMsg)
      }

      // stop on errors
      if (tgtColumns.exists(_.errMsg.isDefined)) {
        val errList = tgtColumns.flatMap(_.errMsg).mkString(", ")
        throw SchemaEvolutionException(s"Data types are different: $errList")
      }

      // log information
      val infoList = tgtColumns.flatMap(_.infoMsg).mkString("\n\t")
      logger.info(s"schema evolution needed. mapping is: \n\t$infoList")
      logger.info(s"old schema: ${oldDf.schema.treeString}")
      logger.info(s"new schema: ${newDf.schema.treeString}")

      // prepare dataframes
      val oldExtendedDf = oldDf.select(tgtColumns.flatMap(_.oldToNewColumn): _*)
      val newExtendedDf = newDf.select(tgtColumns.flatMap(_.newColumn): _*)

      // return
      (oldExtendedDf, newExtendedDf)
    }
  }

  def newColumns(left: SDLDataFrame, right: SDLDataFrame): Seq[String] = {
    schemaColNames(right).diff(schemaColNames(left))
  }

  def deletedColumns(left: SDLDataFrame, right: SDLDataFrame): Seq[String] = {
    schemaColNames(left).diff(schemaColNames(right))
  }

  /**
   * Converts column names to lowercase
   *
   * @param df
   * @return
   */
  def schemaColNames(df: SDLDataFrame): Seq[String] = {
    df.columns
  }

  /**
   * Verifies that two [[SDLDataFrame]]s contain the same columns.
   *
   * @param oldDf
   * @param newDf
   * @return
   */
  def hasSameColNamesAndTypes(oldDf: SDLDataFrame, newDf: SDLDataFrame): Boolean = {
    hasSameColNamesAndTypes(oldDf.schema, newDf.schema)
  }

  def hasSameColNamesAndTypes(oldSchema: SDLStructType, newSchema: SDLStructType): Boolean = {
    getFieldTuples(oldSchema.fields) == getFieldTuples(newSchema.fields)
  }

  def hasSameColNamesAndTypes(oldSchema: Seq[SDLStructField], newSchema: Seq[SDLStructField]): Boolean = {
    getFieldTuples(oldSchema) == getFieldTuples(newSchema)
  }

  /**
   * Converts a col from one SDLDataType to another
   *
   * The following conversion of data types are supported:
   * - numeric type (int, double, float, ...) to string
   * - char and boolean to string
   * - decimal with precision <= 7 to float
   * - decimal with precision <= 16 to double
   * - numerical type to numerical type with higher precision, e.g int to long
   * - delete column in complex type (array, struct, map)
   * - new column in complex type (array, struct, map)
   * - changed data type in complex type (array, struct, map) according to the rules above
   *
   * @param column a Column
   * @param left   original SDLDataType
   * @param right  new SDLDataType
   * @return A column with the transformation expression applied
   */
  def convertSDLDataType(column: SDLColumn, left: SDLDataType, right: SDLDataType, ignoreOldDeletedNestedColumns: Boolean): Option[(SDLColumn, SDLColumn, SDLDataType)] = {
    (left, right) match {
      // simple type
      case (_, _) if isSimpleTypeCastable(left, right) =>
        Some(column.cast(right), column, right)
      // default
      case _ => {
        (left.dataType, right.dataType) match {
          // complex type (Spark)
          case (_: SparkStructType, _: SparkStructType) | (_: SparkArrayType, _: SparkArrayType) | (_: SparkMapType, _: SparkMapType) =>
            val tgtType = ComplexTypeEvolution.consolidateType(left, right, ignoreOldDeletedNestedColumns)
            val udf_convertLeft = ComplexTypeEvolution.schemaEvolutionUdf(left, tgtType)
            val udf_convertRight = ComplexTypeEvolution.schemaEvolutionUdf(right, tgtType)
            Some(udf_convertLeft(column), udf_convertRight(column), tgtType)
          // TODO: Implement Complex Type Evolution for Snowpark types
          // default
          case (_, _) =>
            None
        }
      }
    }
  }

  /**
   * Checks if a SDLDataType is castable to another
   */
  def isSimpleTypeCastable(left: SDLDataType, right: SDLDataType): Boolean = {
    Try(ValueProjector.getSimpleTypeConverter(left, right, Seq())).isSuccess
  }
}
