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

package com.github.saurfang.sas.mapreduce

import java.io.IOException

import com.github.saurfang.sas.parso.ParsoWrapper
import org.apache.commons.io.input.CountingInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.CodecStreams

/**
  * A [[RecordReader]] for [[SasInputFormat]].
  * Each split is aligned to the closest preceding page boundary,
  * calculated from the page size specified in the .sas7bdat meta info.
  */
class SasRecordReader(split: InputSplit,
                      context: TaskAttemptContext) extends RecordReader[NullWritable, Array[Object]] {

  @transient lazy val log = LogManager.getLogger(this.getClass.getName)

  // Process input parameters.
  private val fileSplit = split.asInstanceOf[FileSplit]
  private val filePath = fileSplit.getPath
  private val jobConf = context.getConfiguration

  // Sanity-Check: Ensure file is not compressed.
  private val codec = Option(new CompressionCodecFactory(jobConf).getCodec(filePath))
  private val isSplittable = codec.isEmpty

  // Initialize variables.
  private var recordCount: Long = 0
  private var currentRecordValue: Array[Object] = _

  // Initialize InputStream.
  private val fs = filePath.getFileSystem(jobConf)
  private val rawInputStream = fs.open(filePath)

  private val fileInputStream = codec.map(codec => codec.createInputStream(rawInputStream)).getOrElse(rawInputStream)

  private val countingInputStream = new CountingInputStream(fileInputStream)

  // Initialize Parso SasFileParser.
  private val sasFileReader = ParsoWrapper.createSasFileParser(countingInputStream)

  // Extract static SAS file metadata.
  private val headerLength: Long = sasFileReader.getSasFileProperties.getHeaderLength
  private val pageLength: Long = sasFileReader.getSasFileProperties.getPageLength
  private val pageCount: Long = sasFileReader.getSasFileProperties.getPageCount
  private val columnCount: Long = sasFileReader.getSasFileProperties.getColumnsCount
  private val rowCount: Long = sasFileReader.getSasFileProperties.getRowCount
  private val fileLength: Long = if (isSplittable) {
    fs.getFileStatus(filePath).getLen
  } else {
    headerLength + (pageLength * pageCount)
  }

  // Calculate initial split byte positions.
  private var splitStart: Long = if (isSplittable) {
    fileSplit.getStart
  } else {
    0
  }

  private var splitEnd: Long = if (isSplittable) {
    splitStart + fileSplit.getLength
  } else {
    splitStart + fileLength
  }

  // Log file information
  log.info(s"Reading file of length $fileLength between $splitStart and $splitEnd. ($rowCount rows, $columnCount columns)")

  // Expand splitStart to closest preceding page end.
  if (splitStart > 0) {

    // Calculate how many extra bytes we need to include, so we start on a page boundary.
    val partialPageLength = (splitStart - headerLength) % pageLength

    // Move splitStart back to include these bytes.
    splitStart -= partialPageLength

    if (partialPageLength != 0) {
      log.info(s"Expanded splitStart by $partialPageLength bytes to start on page boundary, splitStart is now: $splitStart.")
    }
  }

  // Shrink splitEnd to closest preceding page end. (Don't move last split, it should end on file end)
  if (splitEnd != fileLength) {

    // Calculate how many bytes we need to exclude, so we end on a page boundary.
    val partialPageLength = (splitEnd - headerLength) % pageLength

    // Move splitEnd back to exclude these bytes.
    splitEnd -= partialPageLength

    if (partialPageLength != 0) {
      log.info(s"Shrunk splitEnd by $partialPageLength bytes to end on page boundary, splitEnd is now: $splitEnd.")
    }
  }

  // Seek input stream. (Don't seek if this is the first split, as it has already read past metadata)
  if (fileInputStream.getPos != splitStart && splitStart > 0) {

    val originalPos = fileInputStream.getPos

    // Shift fileInputStream to start of split.
    fileInputStream.seek(splitStart)
    log.info(s"Shifted fileInputStream to $splitStart offset from $originalPos.")

    // Reset Byte Counter.
    countingInputStream.resetByteCount()

    // If we seek then we need to look at the current page.
    // this is safe because we seeked to a page boundary.
    sasFileReader.readNextPage()
  }

  // Define initialise so we can compile, as it is marked abstract.
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
  }

  override def getCurrentKey: NullWritable = {
    NullWritable.get
  }

  override def getCurrentValue: Array[Object] = {
    currentRecordValue
  }

  override def close() {
    log.info(s"Read $getPos bytes and $recordCount records.")
    if (countingInputStream != null) {
      countingInputStream.close()
    }
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0F
      case _ => Math.min(getPos / (splitEnd - splitStart), 1.0F)
    }
  }

  override def nextKeyValue(): Boolean = {

    // Lazy evaluator to read next record.
    lazy val readNext = {

      // Clear the current stored record.
      currentRecordValue = new Array[Object](columnCount.toInt)

      // Read next record.
      val recordValue: Option[Array[Object]] = Option(sasFileReader.readNext())

      // Store the returned record.
      if (recordValue.isDefined) {
        // copyToArray handles partially corrupted records
        recordValue.get.copyToArray(currentRecordValue)
        recordCount += 1
        true
      }
      else {
        false
      }
    }

    // If there is more to read, read a row.
    if (getPos <= splitEnd - splitStart) {
      readNext
    }
    else {
      false
    }
  }

  // Get byte current byte position of the input stream.
  private def getPos: Long = {
    countingInputStream.getByteCount
  }
}
