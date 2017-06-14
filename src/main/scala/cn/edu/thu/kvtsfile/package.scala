package cn.edu.thu

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object kvtsfile {

  /**
    * add a method to DataFrameReader
    */
  implicit class TSFileDataFrameReader(reader: DataFrameReader) {
    def kvtsfile: String => DataFrame = reader.format("cn.edu.thu.kvtsfile").load
  }
}
