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

private[smartdatalake] trait SDLColumn {
  def column: Any

  def === (other: Any): SDLColumn

  def =!= (other: Any): SDLColumn

  def and (other: SDLColumn): SDLColumn

  def cast(dataType: SDLDataType): SDLColumn

  def isNull: SDLColumn

  def isNotNull: SDLColumn

  def unary_! : SDLColumn

  def as(name: String): SDLColumn
}

private[smartdatalake] case class SparkSDLColumn(override val column: SparkColumn) extends SDLColumn {
  override def ===(other: Any): SDLColumn = column === other

  override def =!=(other: Any): SDLColumn = column === other

  override def and (other: SDLColumn): SDLColumn = column and other

  override def cast(dataType: SDLDataType): SDLColumn = column.cast(dataType)

  override def as(name: String): SDLColumn = column.as(name)

  override def isNull: SDLColumn = column.isNull

  override def isNotNull: SDLColumn = column.isNotNull

  override def unary_! : SDLColumn = column.unary_!
}

private[smartdatalake] case class SnowparkSDLColumn(override val column: SnowparkColumn) extends SDLColumn {
  override def ===(other: Any): SDLColumn = column === other

  override def =!=(other: Any): SDLColumn = column === other

  override def and (other: SDLColumn): SDLColumn = column and other

  override def cast(dataType: SDLDataType): SDLColumn = column.cast(dataType)

  override def as(name: String): SDLColumn = column.as(name)

  override def isNull: SDLColumn = column.isNull

  override def isNotNull: SDLColumn = column.is_not_null

  override def unary_! : SDLColumn = column.column.asInstanceOf[SnowparkColumn].unary_!
}

