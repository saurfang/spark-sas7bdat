// Copyright (C) 2015 Forest Fang.
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

package com.github.saurfang.sas.mapred

import java.io.IOException

import com.github.saurfang.sas.parso.ParsoWrapper
import org.apache.commons.io.input.CountingInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred.{FileSplit, InputSplit, RecordReader}
import org.apache.log4j.LogManager

/**
 * An [[RecordReader]] for [[SasInputFormat]].
 * Each split is aligned to the closest preceding page boundary, calculated from the page size specified in the .sas7bdat meta info.
 */
class SasRecordReader(job: Configuration, split: InputSplit) extends RecordReader[NullWritable, Array[Object]] {
  
  @transient lazy val log = LogManager.getLogger(this.getClass.getName)
    
  //// Warm-ups 
  // Process InputSplit
  private val fileSplit = split.asInstanceOf[FileSplit]
  private val filePath = fileSplit.getPath
  
  // Sanity Check: Ensure file is not compressed.
  private val codec = new CompressionCodecFactory(job).getCodec(filePath)
  if (codec != null) {
    throw new IOException("SASRecordReader does not support reading compressed files.")
  }
  
  
  //// Initialize
  // Variables
  private var recordCount: Int = 0
  private var lastPageBlockCounter: Int = 0
  private var recordValue: Array[Object] = _
  
  // Start/End byte positions for this split
  private var splitStart: Long = fileSplit.getStart
  private var splitEnd: Long = splitStart + fileSplit.getLength

  // Initialize CountingInputStream
  private val fs = filePath.getFileSystem(job)
  private val fileInputStream = fs.open(filePath)
  private val countingInputStream = new CountingInputStream(fileInputStream)

  // Initialize parso SasFileParser
  private val sasFileReader = ParsoWrapper.createSasFileParser(countingInputStream)
  
  // Extract Static SAS-File Metadata
  private val fileLength: Long = fs.getFileStatus(filePath).getLen()
  private val headerLength: Long = sasFileReader.getSasFileProperties.getHeaderLength
  private val pageLength: Long = sasFileReader.getSasFileProperties.getPageLength
  
  
  //// Align splits to page boundaries.
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
  
  // Shrink splitEnd to closest preceding page end. (Don't move last split, it should end on file end.)
  if (splitEnd != fileLength) {
    
    // Calculate how many bytes we need to exclude, so we end on a page boundary.
    val partialPageLength = (splitEnd - headerLength) % pageLength
    
    // Move splitEnd back to exclude these bytes.
    splitEnd -= partialPageLength
    
    if (partialPageLength != 0) { 
      log.info(s"Shrunk splitEnd by $partialPageLength bytes to end on page boundary, splitEnd is now: $splitEnd.") 
    }
  }
  
  // Seek input stream (Don't seek if this is the first split, as it has already read past metadata) 
  val origStreamPos: Long = fileInputStream.getPos()
  if (origStreamPos != splitStart && splitStart != 0) {
    
    // Shift fileInputStream to start of split.
    fileInputStream.seek(splitStart)
    log.info(s"Shifted fileInputStream to $splitStart offset from $origStreamPos.")
    
    // Reset Byte Counter
    countingInputStream.resetByteCount()
    
    // If we seek then we need to look at the current page.
    // this is safe because we seeked to a page boundary.
    sasFileReader.readNextPage()
  }
  
  
  //// Override RecordReader Methods
  // createKey()
  override def createKey: NullWritable = {
    NullWritable.get
  }
  
  // createValue()
  override def createValue: Array[Object] = {
    new Array[Object](sasFileReader.getSasFileProperties.getColumnsCount.toInt)
  }
  
  // getPos()
  override def getPos: Long = {
    countingInputStream.getByteCount
  }
  
  // close()
  override def close() {
    log.info(s"Read $getPos bytes and $recordCount records.")
    if (countingInputStream != null) {
      countingInputStream.close()
    }
  }
  
  // getProgress()
  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0F
      case _ => Math.min(
        (getPos / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }
  
  // next()
  override def next(key: NullWritable, value: Array[Object]): Boolean = {
  
    // Lazy evaluator to read next record.
    lazy val readNext = {
      
      // Read next row.
      val recordValue: Option[Array[Object]] = Option(sasFileReader.readNext(null))
      
      // Store the returned row in the provided value object.
      if (!recordValue.isEmpty) {
        recordValue.get.copyToArray(value)
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
  
}
