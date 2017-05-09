package cn.edu.thu.tsfile.spark

import java.io._
import java.net.URI
import java.util

import cn.edu.thu.tsfile.common.constant.QueryConstant
import cn.edu.thu.tsfile.hadoop.io.HDFSInputStream
import cn.edu.thu.tsfile.spark.DefaultSource._
import cn.edu.thu.tsfile.spark.common.SparkConstant
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant
import cn.edu.thu.tsfile.timeseries.read.query.{QueryDataSet, QueryEngine}
import cn.edu.thu.tsfile.timeseries.read.readSupport.{Field, RowRecord}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * TSFile data source
  *
  * @author QJL
  * @author MXW
  */
class DefaultSource extends FileFormat with DataSourceRegister{

  class TSFileDataSourceException(message: String, cause: Throwable)
    extends Exception(message, cause){
    def this(message: String) = this(message, null)
  }

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  override def inferSchema(
                            spark: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {

    val conf = spark.sparkContext.hadoopConfiguration

    val in = new HDFSInputStream(files.head.getPath, conf)

    val queryEngine = new QueryEngine(in)

    val deltaObjects = queryEngine.getAllDeltaObject
    if(!deltaObjects.get(0).contains(SparkConstant.DELTA_OBJECT_VALUE_SEPARATOR)) {
      throw new TSFileDataSourceException("TsFile delta_object must be in format: key:value(+key:value)*")
    }

    val keys = deltaObjects.get(0).split(SparkConstant.DELTA_OBJECT_SEPARATOR)
      .map(kv => kv.split(SparkConstant.DELTA_OBJECT_VALUE_SEPARATOR)(0))

    keys.foreach(f => {
      DefaultSource.keys += f
    })

    //check if the path is given
    options.getOrElse(DefaultSource.path, throw new TSFileDataSourceException
    (s"${DefaultSource.path} must be specified for tsfile DataSource"))

    //get union series in tsfile
    var tsfileSchema = Converter.getUnionSeries(files, conf)

    Converter.toSparkSqlSchema(tsfileSchema, keys)
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    true
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

          partitionSchema.foreach(f => println(f))

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {

      val conf = broadcastedConf.value.value
      val in = new HDFSInputStream(new Path(new URI(file.filePath)), conf)

      var taskInfo: String = ""
      Option(TaskContext.get()).foreach { taskContext => {
        taskContext.addTaskCompletionListener { _ => in.close() }
        taskInfo += "task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId()}
      }
      DefaultSource.logger.info("taskInfo: {}", taskInfo)

      val parameters = new util.HashMap[java.lang.String, java.lang.Long]()
      parameters.put(QueryConstant.PARTITION_START_OFFSET, file.start.asInstanceOf[java.lang.Long])
      parameters.put(QueryConstant.PARTITION_END_OFFSET, (file.start + file.length).asInstanceOf[java.lang.Long])

      //convert tsfilequery to QueryConfigs
      val queryConfigs = Converter.toQueryConfigs(in, requiredSchema, filters, keys.toArray,
        file.start.asInstanceOf[java.lang.Long], (file.start + file.length).asInstanceOf[java.lang.Long])

      //use QueryConfigs to query tsfile
      val dataSets = Executor.query(in, queryConfigs.toList, parameters)

      case class Record(record: RowRecord, index: Int)

      implicit object RowRecordOrdering extends Ordering[Record] {
        override def compare(r1: Record, r2: Record): Int = {
          if(r1.record.timestamp == r2.record.timestamp) {
            r1.record.getFields.get(0).deltaObjectId.compareTo(r2.record.getFields.get(0).deltaObjectId)
          } else if(r1.record.timestamp < r2.record.timestamp){
            1
          } else {
            -1
          }
        }
      }

      val priorityQueue = new mutable.PriorityQueue[Record]()

      //init priorityQueue with first record of each dataSet
      var queryDataSet: QueryDataSet = null
      for(i <- 0 until dataSets.size()) {
        queryDataSet = dataSets(i)
        if(queryDataSet.hasNextRecord) {
          val rowRecord = queryDataSet.getNextRecord
          priorityQueue.enqueue(Record(rowRecord, i))
        }
      }

      var curRecord: Record = null

      new Iterator[InternalRow] {
        private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

        private val safeDataRow = new GenericRow(rowBuffer)

        // Used to convert `Row`s containing data columns into `InternalRow`s.
        private val encoderForDataColumns = RowEncoder(requiredSchema)

        private val keyMap = new mutable.HashMap[String, String]()

        override def hasNext: Boolean = {
          var hasNext = false

          while (priorityQueue.nonEmpty && !hasNext) {
            //get a record from priorityQueue
            var tmpRecord = priorityQueue.dequeue()
            //insert a record to priorityQueue
            if (dataSets(tmpRecord.index).hasNextRecord) {
              priorityQueue.enqueue(Record(dataSets(tmpRecord.index).getNextRecord, tmpRecord.index))
            }

            if (curRecord == null || tmpRecord.record.timestamp != curRecord.record.timestamp ||
              !tmpRecord.record.getFields.get(0).deltaObjectId.equals(curRecord.record.getFields.get(0).deltaObjectId)) {
              curRecord = tmpRecord

              val deltaObjectId = curRecord.record.fields.get(0).deltaObjectId
              val kvs = deltaObjectId.split(SparkConstant.DELTA_OBJECT_SEPARATOR)
              kvs.foreach(kv => {
                val k_v = kv.split(SparkConstant.DELTA_OBJECT_VALUE_SEPARATOR)
                keyMap.put(k_v(0), k_v(1))
              })

              hasNext = true
            }
          }

          hasNext
        }

        override def next(): InternalRow = {

          val fields = new scala.collection.mutable.HashMap[String, Field]()
          for (i <- 0 until curRecord.record.fields.size()) {
            val field = curRecord.record.fields.get(i)
            fields.put(field.measurementId, field)
          }

          //index in one required row
          var index = 0
          requiredSchema.foreach((field: StructField) => {
            if (field.name == SQLConstant.RESERVED_TIME) {
              rowBuffer(index) = curRecord.record.timestamp
            } else if (keys.contains(field.name)) {
              rowBuffer(index) = keyMap.get(field.name).orNull
            } else {
              rowBuffer(index) = Converter.toSqlValue(fields.getOrElse(field.name, null))
            }
            index += 1
          })

          encoderForDataColumns.toRow(safeDataRow)
        }
      }
    }
  }

  override def shortName(): String = "tsfile"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job, options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    null
  }

}


object DefaultSource {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  val path = "path"
  val keys = new ArrayBuffer[String]()

  class SerializableConfiguration(@transient var value: Configuration) extends Serializable with KryoSerializable{
    override def write(kryo: Kryo, output: Output): Unit = {
      val dos = new DataOutputStream(output)
      value.write(dos)
      dos.flush()
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      value = new Configuration(false)
      value.readFields(new DataInputStream(input))
    }
  }

}
