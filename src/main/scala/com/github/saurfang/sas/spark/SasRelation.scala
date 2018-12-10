// Copyright (C) 2018 Forest Fang.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.github.saurfang.sas.spark

import java.io.IOException

import com.epam.parso.ColumnFormat
import com.epam.parso.impl.SasFileReaderImpl
import com.github.saurfang.sas.mapreduce.SasInputFormat
import com.github.saurfang.sas.parso.ParsoWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Defines a RDD that is backed by [[SasInputFormat]]. Data are coerced into appropriate types according to
  * meta information embedded in .sas7bdat file.
  */
case class SasRelation protected[spark](
                                         location: String,
                                         userSchema: StructType = null,
                                         extractLabel: Boolean = false,
                                         forceLowercaseNames: Boolean = false,
                                         inferDecimal: Boolean = false,
                                         inferDecimalScale: Option[Int] = None,
                                         inferFloat: Boolean = false,
                                         inferInt: Boolean = false,
                                         inferLong: Boolean = false,
                                         inferShort: Boolean = false,
                                         metadataTimeout: Int = 60,
                                         minSplitSize: Option[Long] = None,
                                         maxSplitSize: Option[Long] = None
                                       )(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  @transient lazy val log = LogManager.getLogger(this.getClass.getName)

  // Automatically extract schema from file.
  val schema: StructType = inferSchema(
    extractLabel = extractLabel,
    forceLowercaseNames = forceLowercaseNames,
    inferDecimal = inferDecimal,
    inferDecimalScale = inferDecimalScale,
    inferFloat = inferFloat,
    inferInt = inferInt,
    inferLong = inferLong,
    inferShort = inferShort,
    metadataTimeout = metadataTimeout
  )

  override def buildScan: RDD[Row] = {

    // Start from the hadoopConfiguration in sparkContext.
    val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

    // Set min/max split sizes if provided.
    if (minSplitSize.isDefined) {conf.setLong("mapred.min.split.size", minSplitSize.get)}
    if (maxSplitSize.isDefined) {conf.setLong("mapred.max.split.size", maxSplitSize.get)}

    // Read an RDD with NullWritable keys, Array[Object] values, with SasInputFormat format.
    // Then use map to extract every value from the returned RDD of (key, value) tuples.
    val baseRDD: RDD[Array[Object]] = {
      sqlContext.sparkContext.newAPIHadoopFile(
        path = location,
        fClass = classOf[SasInputFormat],
        kClass = classOf[NullWritable],
        vClass = classOf[Array[Object]],
        conf = conf
      ).map(_._2)
    }

    // Convert our RDD[Array[Object]] into RDD[Row]
    baseRDD.mapPartitions { rowIterator => parseSAS(rowIterator, schema.fields) }
  }

  private def parseSAS(rowIterator: Iterator[Array[Object]], schemaFields: Seq[StructField]): Iterator[Row] = {

    var isFirstRow: Boolean = true

    rowIterator.map { rowArray =>

      // Check that the right number of columns were passed as schema.
      if (isFirstRow) {
        val numColsRead: Int = rowArray.length
        val numColsSchema: Int = schemaFields.length

        if (numColsRead != numColsSchema) {
          throw new IOException(s"Provided schema has $numColsSchema but SAS file has $numColsRead columns.")
        }
        isFirstRow = false
      }

      // Conform parso's returned array of {Date, Double, Long, Int, String} into the provided schema.
      val conformedRowArray: Array[Any] = rowArray.zipWithIndex.map { case (elementValue, index) =>
        elementValue match {
          case null => {
            schemaFields(index).nullable match {
              case true => null
              case false => throw new IOException(s"Column: '${schemaFields(index).name}' (at index: $index)," +
                s" contains a null value but the provided schema specified it as non-nullable.")
            }
          }
          case x: java.lang.Number => {
            schemaFields(index).dataType match {
              case DecimalType() => new java.math.BigDecimal(x.toString)
              case DoubleType => x.doubleValue
              case FloatType => x.floatValue
              case IntegerType => x.intValue
              case LongType => x.longValue
              case ShortType => x.shortValue
              case _ => throw new IOException(s"Column: '${schemaFields(index).name}' (at index: $index), cannot" +
                s" have DataType: '${schemaFields(index).dataType}', found value: $x")
            }
          }
          case x: java.lang.String => {
            schemaFields(index).dataType match {
              case StringType => x
              case _ => throw new IOException(s"Column: '${schemaFields(index).name}' (at index: $index), cannot" +
                s" have DataType: '${schemaFields(index).dataType}', found value: $x")
            }
          }
          case x: java.util.Date => {
            schemaFields(index).dataType match {
              case TimestampType => new java.sql.Timestamp(x.getTime)
              case DateType => new java.sql.Date(x.getTime)
              case _ => throw new IOException(s"Column: '${schemaFields(index).name}' (at index: $index), cannot" +
                s" have DataType: '${schemaFields(index).dataType}', found value: $x")
            }
          }
        }
      }
      Row.fromSeq(conformedRowArray)
    }
  }

  private def inferSchema(
                           extractLabel: Boolean,
                           forceLowercaseNames: Boolean,
                           inferDecimal: Boolean,
                           inferDecimalScale: Option[Int],
                           inferFloat: Boolean,
                           inferInt: Boolean,
                           inferLong: Boolean,
                           inferShort: Boolean,
                           metadataTimeout: Int
                         ): StructType = {

    if (this.userSchema != null) {
      userSchema
    } else {

      // Open a reader so we can get the metadata.
      val conf = sqlContext.sparkContext.hadoopConfiguration
      val path = new Path(location)
      val fs = path.getFileSystem(conf)
      val inputStream = fs.open(path)
      val sasFileReaderFuture = future {
        new SasFileReaderImpl(inputStream)
      }

      // Corrupt files hang, so we need to time out.
      val sasFileReader: SasFileReaderImpl = {
        try {
          Await.result(sasFileReaderFuture, metadataTimeout seconds)
        } catch {
          case e: TimeoutException => throw new TimeoutException(s"Timed out after $metadataTimeout sec while " +
            s"reading file metadata, file might be corrupt. (Change timeout with 'metadataTimeout' paramater)")
        }
      }

      // Throw an error if the file has no columns.
      val columnCount: Long = sasFileReader.getSasFileProperties.getColumnsCount
      if (columnCount == 0) {
        throw new IOException("SAS file has no columns, or might not be a valid SAS file.")
      }

      // Retrieve some SAS constants.
      val DATE_FORMAT_STRINGS = ParsoWrapper.DATE_FORMAT_STRINGS
      val DATE_TIME_FORMAT_STRINGS = ParsoWrapper.DATE_TIME_FORMAT_STRINGS

      // Create a buffer of ShemaFields corresponding to the SAS metadata.
      val schemaFields = sasFileReader.getColumns.map { column =>

        // Retrieve general info about current column.
        val columnClass: Class[_] = column.getType
        val columnName: String = if (forceLowercaseNames) {
          column.getName.toLowerCase
        } else {
          column.getName
        }
        val columnLabel: Option[String] = if (extractLabel) {
          if (!column.getLabel.isEmpty) {
            Option(column.getLabel)
          } else {
            None
          }
        } else {
          None
        }
        val columnLength: Int = column.getLength

        // Retrieve format info about current column.
        val columnFormat: ColumnFormat = column.getFormat
        val columnFormatName: String = columnFormat.getName
        val columnFormatWidth: Int = columnFormat.getWidth
        val columnFormatPrecision: Int = columnFormat.getPrecision

        // Map SAS column types to Spark types.
        val columnSparkType: DataType = {
          if (columnClass == classOf[Number]) {
            if (DATE_TIME_FORMAT_STRINGS.contains(columnFormatName)) {
              TimestampType
            } else if (DATE_FORMAT_STRINGS.contains(columnFormatName)) {
              DateType
            } else if (columnFormatPrecision == 0 && columnFormatWidth != 0) {
              columnLength match {
                case l if (inferShort && l <= 2) => ShortType
                case l if (inferInt && l <= 4) => IntegerType
                case l if (inferLong && l <= 8) => LongType
                case _ => DoubleType
              }
            } else if (inferDecimal && columnFormatPrecision >= 1 && columnFormatWidth != 0) {
              DecimalType(inferDecimalScale.getOrElse(columnFormatWidth), columnFormatPrecision)
            } else if (inferFloat && columnLength <= 4) {
              FloatType
            } else {
              DoubleType
            }
          } else {
            StringType
          }
        }

        // Return a struct field for this column.
        if (columnLabel.isEmpty) {
          StructField(columnName, columnSparkType, nullable = true)
        } else {
          StructField(columnName, columnSparkType, nullable = true).withComment(columnLabel.get)
        }

      }
      inputStream.close()
      StructType(schemaFields)
    }
  }
}
