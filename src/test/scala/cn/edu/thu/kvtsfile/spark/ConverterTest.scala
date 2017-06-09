package cn.edu.thu.kvtsfile.spark

import java.io.File
import java.util

import cn.edu.thu.kvtsfile.spark.common.{SQLConstant, SparkConstant}
import cn.edu.thu.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema
import cn.edu.thu.tsfile.timeseries.read.query.QueryConfig
import cn.edu.thu.tsfile.timeseries.read.readSupport.Field
import org.apache.spark.sql.sources.{Filter, GreaterThan, LessThan, Or}
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author QJL
  */
class ConverterTest extends FunSuite with BeforeAndAfterAll {

  private val tsfilePath: String = "src/test/resources/test.tsfile"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val tsfile = new File(tsfilePath)
    if(!tsfile.getParentFile.exists())
      tsfile.mkdirs()
    new CreateKmxTSFile().createTSFile2(tsfilePath)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  test("testToQueryConfigs") {
    val in = new LocalFileInput(tsfilePath)

    val requiredSchema = StructType(Seq(
      StructField("s1", IntegerType, nullable = true)
    ))

    val filters = new ListBuffer[Filter]()
    filters += LessThan("time", 80)
    filters += GreaterThan("time", 50)
    filters += Or(LessThan("s1", 50), GreaterThan("s1", 80))

    val keys = new ArrayBuffer[String]()
    keys += ""
    val queryConfigs = Converter.toQueryConfigs(in, requiredSchema, filters, keys.toArray, "0".toLong, "749".toLong)

    val queryConfig0 = new QueryConfig("D:d3+C:c3.s1", "0,(<80)&(>50)", "null", "2,D:d3+C:c3.s1,<50")
    val queryConfig1 = new QueryConfig("D:d3+C:c2.s1", "0,(<80)&(>50)", "null", "2,D:d3+C:c2.s1,<50")
    val queryConfig2 = new QueryConfig("D:d3+C:c1.s1", "0,(<80)&(>50)", "null", "2,D:d3+C:c1.s1,<50")
    val queryConfig3 = new QueryConfig("D:d3+C:c3.s1", "0,(<80)&(>50)", "null", "2,D:d3+C:c3.s1,>80")
    val queryConfig4 = new QueryConfig("D:d3+C:c2.s1", "0,(<80)&(>50)", "null", "2,D:d3+C:c2.s1,>80")
    val queryConfig5 = new QueryConfig("D:d3+C:c1.s1", "0,(<80)&(>50)", "null", "2,D:d3+C:c1.s1,>80")

    Assert.assertEquals(6, queryConfigs.length)
    println(queryConfigs)

    Assert.assertEquals(queryConfig0.getSelectColumns, queryConfigs(0).getSelectColumns)
    Assert.assertEquals(queryConfig0.getValueFilter, queryConfigs(0).getValueFilter)
    Assert.assertEquals(queryConfig0.getTimeFilter, queryConfigs(0).getTimeFilter)

    Assert.assertEquals(queryConfig1.getSelectColumns, queryConfigs(1).getSelectColumns)
    Assert.assertEquals(queryConfig1.getValueFilter, queryConfigs(1).getValueFilter)
    Assert.assertEquals(queryConfig1.getTimeFilter, queryConfigs(1).getTimeFilter)

    Assert.assertEquals(queryConfig2.getSelectColumns, queryConfigs(2).getSelectColumns)
    Assert.assertEquals(queryConfig2.getValueFilter, queryConfigs(2).getValueFilter)
    Assert.assertEquals(queryConfig2.getTimeFilter, queryConfigs(2).getTimeFilter)

    Assert.assertEquals(queryConfig3.getSelectColumns, queryConfigs(3).getSelectColumns)
    Assert.assertEquals(queryConfig3.getValueFilter, queryConfigs(3).getValueFilter)
    Assert.assertEquals(queryConfig3.getTimeFilter, queryConfigs(3).getTimeFilter)

    Assert.assertEquals(queryConfig4.getSelectColumns, queryConfigs(4).getSelectColumns)
    Assert.assertEquals(queryConfig4.getValueFilter, queryConfigs(4).getValueFilter)
    Assert.assertEquals(queryConfig4.getTimeFilter, queryConfigs(4).getTimeFilter)

    Assert.assertEquals(queryConfig5.getSelectColumns, queryConfigs(5).getSelectColumns)
    Assert.assertEquals(queryConfig5.getValueFilter, queryConfigs(5).getValueFilter)
    Assert.assertEquals(queryConfig5.getTimeFilter, queryConfigs(5).getTimeFilter)
  }

  test("testToSparkSqlSchema") {
    val fields : util.ArrayList[SeriesSchema]= new util.ArrayList[SeriesSchema]()
    fields.add(new SeriesSchema("s1", TSDataType.INT32, TSEncoding.PLAIN))
    fields.add(new SeriesSchema("s2", TSDataType.INT64, TSEncoding.PLAIN))
    fields.add(new SeriesSchema("s3", TSDataType.FLOAT, TSEncoding.PLAIN))
    fields.add(new SeriesSchema("s4", TSDataType.DOUBLE, TSEncoding.PLAIN))
    fields.add(new SeriesSchema("s5", TSDataType.BOOLEAN, TSEncoding.PLAIN))
    fields.add(new SeriesSchema("s6", TSDataType.BYTE_ARRAY, TSEncoding.PLAIN))
    val keys = "D:d1+C:c1+V:v1".split(SparkConstant.DELTA_OBJECT_SEPARATOR).map(kv => kv.split(SparkConstant.DELTA_OBJECT_VALUE_SEPARATOR)(0))
    val sqlSchema = Converter.toSparkSqlSchema(fields, keys)

    val expectedFields = Array(
      StructField(SQLConstant.RESERVED_TIME, LongType, nullable = true),
      StructField("D", StringType, nullable = true),
      StructField("C", StringType, nullable = true),
      StructField("V", StringType, nullable = true),
      StructField("s1", IntegerType, nullable = true),
      StructField("s2", LongType, nullable = true),
      StructField("s3", FloatType, nullable = true),
      StructField("s4", DoubleType, nullable = true),
      StructField("s5", BooleanType, nullable = true),
      StructField("s6", BinaryType, nullable = true)
    )
    val expectedType = StructType(expectedFields)
    val expectedSchema = StructType(expectedType.toList)

    Assert.assertEquals(expectedSchema, sqlSchema.get)
  }

  test("testToSqlValue") {
    val boolField = new Field(TSDataType.BOOLEAN, "s1")
    boolField.setBoolV(true)
    val intField = new Field(TSDataType.INT32, "s1")
    intField.setIntV(32)
    val longField = new Field(TSDataType.INT64, "s1")
    longField.setLongV(64l)
    val floatField = new Field(TSDataType.FLOAT, "s1")
    floatField.setFloatV(3.14f)
    val doubleField = new Field(TSDataType.DOUBLE, "s1")
    doubleField.setDoubleV(0.618d)

    Assert.assertEquals(Converter.toSqlValue(boolField), true)
    Assert.assertEquals(Converter.toSqlValue(intField), 32)
    Assert.assertEquals(Converter.toSqlValue(longField), 64l)
    Assert.assertEquals(Converter.toSqlValue(floatField), 3.14f)
    Assert.assertEquals(Converter.toSqlValue(doubleField), 0.618d)
  }

}
