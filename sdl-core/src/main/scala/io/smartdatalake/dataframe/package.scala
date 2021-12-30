package io.smartdatalake

package object dataframe {
  type SnowparkCaseExpression = com.snowflake.snowpark.CaseExpr
  type SparkDataFrame = org.apache.spark.sql.DataFrame
  type SnowparkDataFrame = com.snowflake.snowpark.DataFrame
  type SparkColumn = org.apache.spark.sql.Column
  type SnowparkColumn = com.snowflake.snowpark.Column
  type SparkStructType = org.apache.spark.sql.types.StructType
  type SnowparkStructType = com.snowflake.snowpark.types.StructType
  type SparkArrayType = org.apache.spark.sql.types.ArrayType
  type SnowparkArrayType = com.snowflake.snowpark.types.ArrayType
  type SparkMapType = org.apache.spark.sql.types.MapType
  type SparkStructField = org.apache.spark.sql.types.StructField
  type SnowparkStructField = com.snowflake.snowpark.types.StructField
  type SparkDataType = org.apache.spark.sql.types.DataType
  type SnowparkDataType = com.snowflake.snowpark.types.DataType

  implicit def sdlColumn2SparkColumn(sdlColumn: SDLColumn): SparkColumn = {
    sdlColumn.column match {
      case sparkColumn: SparkColumn => sparkColumn
      case _ => throw new Exception("Invalid cast.")
    }
  }

  implicit def sdlColumn2SnowparkColumn(sdlColumn: SDLColumn): SnowparkColumn = {
    sdlColumn.column match {
      case snowparkColumn: SnowparkColumn => snowparkColumn
      case _ => throw new Exception("Invalid cast.")
    }
  }

  implicit def sdlDataType2SparkDataType(sdlDataType: SDLDataType): SparkDataType = {
    sdlDataType.dataType match {
      case sparkDataType: SparkDataType => sparkDataType
      case _ => throw new Exception("Invalid cast.")
    }
  }

  implicit def sdlDataType2SnowparkDataType(sdlDataType: SDLDataType): SnowparkDataType = {
    sdlDataType.dataType match {
      case snowparkDataType: SnowparkDataType => snowparkDataType
      case _ => throw new Exception("Invalid cast.")
    }
  }

  implicit def sdlDataFrame2SparkDataFrame(sdlDataFrame: SDLDataFrame): SparkDataFrame = {
    sdlDataFrame.dataFrame match {
      case sparkDataFrame: SparkDataFrame => sparkDataFrame
      case _ => throw new Exception("Invalid cast.")
    }
  }

  implicit def sdlDataFrame2SnowparkDataFrame(sdlDataFrame: SDLDataFrame): SnowparkDataFrame = {
    sdlDataFrame.dataFrame match {
      case snowparkDataFrame: SnowparkDataFrame => snowparkDataFrame
      case _ => throw new Exception("Invalid cast.")
    }
  }

  implicit def sparkDataType2SDLDataType(sparkDataType: SparkDataType): SDLDataType = {
    SparkSDLDataType(sparkDataType)
  }

  implicit def sparkDataType2SDLDataType(sparkDataType: SnowparkDataType): SDLDataType = {
    SnowparkSDLDataType(sparkDataType)
  }

  implicit def sparkColumn2SparkSDLColumn(sparkColumn: SparkColumn): SDLColumn = {
    SparkSDLColumn(sparkColumn)
  }

  implicit def snowparkColumn2SnowparkSDLColumn(snowparkColumn: SnowparkColumn): SDLColumn = {
    SnowparkSDLColumn(snowparkColumn)
  }

  implicit def sparkDataFrame2SparkSDLDataFrame(sparkDataFrame: SparkDataFrame): SDLDataFrame = {
    SparkSDLDataFrame(sparkDataFrame)
  }

  implicit def snowparkDataFrame2SnowparkSDLDataFrame(snowparkDataFrame: SnowparkDataFrame): SDLDataFrame = {
    SnowparkSDLDataFrame(snowparkDataFrame)
  }

  implicit def sparkStructType2SparkSDLStructType(sparkStructType: SparkStructType): SDLStructType = {
    SparkSDLStructType(sparkStructType)
  }

  implicit def snowparkStructType2SnowparkSDLStructType(snowparkStructType: SnowparkStructType): SDLStructType = {
    SnowparkSDLStructType(snowparkStructType)
  }

  implicit def sparkStructField2SparkSDLStructField(sparkStructField: SparkStructField): SDLStructField = {
    SparkSDLStructField(sparkStructField)
  }

  implicit def snowparkStructField2SnowparkSDLStructField(snowparkStructField: SnowparkStructField): SDLStructField = {
    SnowparkSDLStructField(snowparkStructField)
  }

  implicit def sdlColumns2SparkColumns(sdlColumns: Seq[SDLColumn]): Seq[SparkColumn] = {
    sdlColumns.map((sdlColumn: SDLColumn) => {
      sdlColumn match {
        case sparkSDLColumn: SparkSDLColumn => sparkSDLColumn.column
      }
    })
  }

  implicit def sdlColumns2SnowparkColums(sdlColumns: Seq[SDLColumn]): Seq[SnowparkColumn] = {
    sdlColumns.map((sdlColumn: SDLColumn) => {
      sdlColumn match {
        case snowparkSDLColumn: SnowparkSDLColumn => snowparkSDLColumn.column
      }
    })
  }

  implicit def sparkColumns2SDLColumns(sparkColumns: Seq[SparkColumn]): Seq[SDLColumn] = {
    sparkColumns.map((sparkColumn: SparkColumn) => {
      SparkSDLColumn(sparkColumn)
    })
  }

  implicit def snowparkColumns2SDLColumns(snowparkColumns: Seq[SnowparkColumn]): Seq[SDLColumn] = {
    snowparkColumns.map((snowparkColumn: SnowparkColumn) => {
      SnowparkSDLColumn(snowparkColumn)
    })
  }

  implicit def snowparkCaseExpression2SDLCaseExpression(snowparkCaseExpression: SnowparkCaseExpression): SDLCaseExpression = {
    SnowparkSDLCaseExpression(snowparkCaseExpression)
  }

  implicit def sparkColumn2SDLCaseExpression(sparkColumn: SparkColumn): SDLCaseExpression = {
    SparkSDLCaseExpression(sparkColumn)
  }

}

