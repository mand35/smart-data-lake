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

package io.smartdatalake.dataframe

import com.snowflake.snowpark.SnowparkHelper

private[smartdatalake] trait SDLStructType extends Seq[SDLStructField] {
  def apply(colName: String): SDLStructField

  def fields: Seq[SDLStructField]

  def treeString: String

  override def length: Int = fields.length

  override def iterator: Iterator[SDLStructField] = fields.iterator
}

private[smartdatalake] case class SparkSDLStructType(structType: SparkStructType) extends SDLStructType {
  override def apply(colName: String): SDLStructField = structType(colName)

  override def apply(idx: Int): SDLStructField = structType.fields(idx)

  override def fields: Seq[SparkSDLStructField] = structType.fields.map(field => SparkSDLStructField(field))

  override def treeString: String = structType.treeString

}

private[smartdatalake] case class SnowparkSDLStructType(structType: SnowparkStructType) extends SDLStructType {
  override def apply(colName: String): SDLStructField = structType(colName)

  override def apply(idx: Int): SDLStructField = structType.fields(idx)

  override def fields: Seq[SnowparkSDLStructField] = structType.fields.map(field => SnowparkSDLStructField(field))

  override def treeString: String = SnowparkHelper.RichStructType(structType).publicTreeString
}

