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
package io.smartdatalake.definitions

import java.time.LocalDateTime


/**
  * Hive conventions
  */
object HiveConventions {

  /**
   * Provides timestamp with meaning "not expired", "valid", "without expiry date"
   *
   * @return timestamp with symbolic date 31.12.9999
   */
  def getHistorizationSurrogateTimestamp: LocalDateTime = {
    //val surrogateTimestamp = new java.sql.Timestamp(new Date("9999/12/31").getTime)
    val surrogateTimestamp = LocalDateTime.of(9999, 12, 31, 0, 0, 0, 0)
    surrogateTimestamp
  }
}
