package cn.edu.thu.kvtsfile

import java.io.File

import cn.edu.thu.kvtsfile.io.CreateKmxTSFile
import cn.edu.thu.kvtsfile.qp.common.SQLConstant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * @author QJL
  */
class TSFileSuit extends FunSuite with BeforeAndAfterAll {

  private val resourcesFolder = "src/test/resources"
  private val kmxTsfile = "src/test/resources/kmx.tsfile"
  private val tsfileFolder = "src/test/resources/tsfile"
  private val tsfilePath1 = "src/test/resources/tsfile/test1.tsfile"
  private val tsfilePath2 = "src/test/resources/tsfile/test2.tsfile"
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val resources = new File(resourcesFolder)
    if(!resources.exists())
      resources.mkdirs()
    val tsfile_folder = new File(tsfileFolder)
    if(!tsfile_folder.exists())
      tsfile_folder.mkdirs()
    new CreateKmxTSFile().createTSFile1(tsfilePath1)
    new CreateKmxTSFile().createTSFile2(tsfilePath2)
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("testKmxTsfile") {
    val df = spark.read.format("cn.edu.thu.kvtsfile").load(kmxTsfile)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    newDf.show()
  }

  test("testMultiFiles") {
    val df = spark.read.format("cn.edu.thu.kvtsfile").load(tsfileFolder)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where D = 'd1' and C = 'c1' or D = 'd3'")
    Assert.assertEquals(5, newDf.count())
  }

  test("test key") {
    val df = spark.read.kvtsfile(tsfilePath1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table where C = 'c1' and D = 'd1'")
    val count = newDf.count()
    Assert.assertEquals(2, count)
  }

  test("test Select *") {
    val df = spark.read.kvtsfile(tsfilePath1)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table")
    val count = newDf.count()
    Assert.assertEquals(8, count)
  }


  test("testQuerySchema") {
    val df = spark.read.kvtsfile(tsfilePath1)

    val expected = StructType(Seq(
      StructField(SQLConstant.RESERVED_TIME, LongType, nullable = true),
      StructField("D", StringType, nullable = true),
      StructField("C", StringType, nullable = true),
      StructField("V", StringType, nullable = true),
      StructField("s3", FloatType, nullable = true),
      StructField("s4", DoubleType, nullable = true),
      StructField("s1", IntegerType, nullable = true),
      StructField("s2", LongType, nullable = true)
    ))
    Assert.assertEquals(expected, df.schema)
  }

}