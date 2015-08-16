package com.github.saurfang.sas.spark

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.ggasoftware.parso.{SasFileConstants, SasFileReader}
import com.github.saurfang.sas.mapred.SasInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.Logging
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

/**
 * Defines a RDD that is backed by [[SasInputFormat]]. Data are coerced into approriate types according to
 * meta information embedded in .sas7bdat file.
 */
case class SasRelation protected[spark](
                                         location: String,
                                         userSchema: StructType = null,
                                         @transient conf: JobConf = new JobConf(),
                                         minPartitions: Int = 0
                                         )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging {
  val schema = inferSchema()

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  def buildScan() = {
    FileInputFormat.setInputPaths(conf, new Path(location))
    val baseRDD = new HadoopRDD[NullWritable, Array[Object]](
      sqlContext.sparkContext,
      conf,
      classOf[SasInputFormat],
      classOf[NullWritable],
      classOf[Array[Object]],
      minPartitions
    ).map(_._2)

    val numFields = schema.fields.length
    val row = new GenericMutableRow(numFields)

    baseRDD.mapPartitions { iter => parseSAS(iter, schema.fields, row) }
  }

  private def parseSAS(
                        iter: Iterator[Array[Object]],
                        schemaFields: Seq[StructField],
                        row: GenericMutableRow): Iterator[Row] = {
    iter.flatMap { records =>
      var index: Int = 0
      try {
        if (records.isEmpty) {
          logWarning(s"Ignoring empty line: $records")
          None
        } else {
          index = 0

          while (index < schemaFields.length) {
            row(index) = records(index) match {
              //SAS itself only has double as its numeric type.
              //Hence we can't infer Long/Integer type ahead of time therefore we convert it back to Double
              case x: java.lang.Long => x.toDouble
              //TODO: Return proper Date object once SparkSQL/parquet adds support for DateType
              case x: java.util.Date =>
                schemaFields(index).dataType match {
                  case TimestampType => new java.sql.Timestamp(x.getTime)
                  case _ =>
                    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
                    dateFormat.format(x)
                }
              case x => x
            }
            index = index + 1
          }

          Some(row)
        }
      } catch {
        case aiob: ArrayIndexOutOfBoundsException =>
          (index until schemaFields.length).foreach(ind => row(ind) = null)
          Some(row)
        case NonFatal(e) =>
          logError(s"Exception while parsing line: ${records.toList}.", e)
          None
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val path = new Path(location)
      val fs = path.getFileSystem(conf)
      val inputStream = fs.open(path)
      val sasFileReader = new SasFileReader(inputStream)
      import scala.collection.JavaConversions._
      val schemaFields = sasFileReader.getColumns.map { column =>
        val columnType =
          if (column.getType == classOf[Number]) {
            if (SasFileConstants.DATE_TIME_FORMAT_STRINGS.contains(column.getFormat))
              TimestampType
            else if (SasFileConstants.DATE_FORMAT_STRINGS.contains(column.getFormat))
              StringType //TODO: Change to DateType when parquet supports it
            else
              DoubleType
          } else {
            StringType
          }
        StructField(column.getName, columnType, nullable = true)
      }
      inputStream.close()
      StructType(schemaFields)
    }
  }
}
