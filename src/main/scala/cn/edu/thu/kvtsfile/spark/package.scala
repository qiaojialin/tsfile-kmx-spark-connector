package cn.edu.thu.kvtsfile

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object spark {

  /**
    * add a method to DataFrameReader
    */
  implicit class TSFileDataFrameReader(reader: DataFrameReader) {
    def kvtsfile: String => DataFrame = reader.format("cn.edu.thu.kvtsfile.spark").load
  }
}
