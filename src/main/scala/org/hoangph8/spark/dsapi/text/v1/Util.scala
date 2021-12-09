package org.hoangph8.spark.dsapi.text.v1

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

object Util {
  def cast(value: String, dataType: DataType) = {
    dataType match {
      case _: IntegerType => if (value.equals("")) 0 else value.toInt
      case _: LongType => if (value.equals("")) 0 else value.toLong
      case _: StringType => value
    }
  }
}
