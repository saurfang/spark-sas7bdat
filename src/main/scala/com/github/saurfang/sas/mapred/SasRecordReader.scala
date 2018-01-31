// Copyright (C) 2011-2012 the original author or authors.
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
 * An [[RecordReader]] for [[SasInputFormat]]. Each split is aligned to page sized specified in the .sas7bdat meta info.
 * Each split looks back previous split if the start of split is incomplete.
 */
class SasRecordReader(job: Configuration, split: InputSplit) extends RecordReader[NullWritable, Array[Object]] {

  private var recordCount: Int = 0
  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var lastPageBlockCounter: Int = 0
  private var recordValue: Array[Object] = _

  @transient lazy val log = LogManager.getLogger(this.getClass.getName)

  // the file input
  private val fileSplit = split.asInstanceOf[FileSplit]

  // the byte position this fileSplit starts at
  splitStart = fileSplit.getStart

  // splitEnd byte marker that the fileSplit ends at
  splitEnd = splitStart + fileSplit.getLength

  // the actual file we will be reading from
  private val file = fileSplit.getPath
  // check compression
  private val codec = new CompressionCodecFactory(job).getCodec(file)
  if (codec != null) {
    throw new IOException("SASRecordReader does not support reading compressed files")
  }
  // get the filesystem
  private val fs = file.getFileSystem(job)
  // open the File
  private val fileInputStream = fs.open(file)
  private val countingInputStream = new CountingInputStream(fileInputStream)
  // open SAS file reader to read meta data
  private val sasFileReader = ParsoWrapper.createSasFileParser(countingInputStream)

  log.info(sasFileReader.getSasFileProperties.toString)

  private val maxPagePosition: Long = sasFileReader.getSasFileProperties.getHeaderLength +
    sasFileReader.getSasFileProperties.getPageCount * sasFileReader.getSasFileProperties.getPageLength

  // align splitEnd to pages
  // only shrink splitEnd if this is the first split or the split starts after meta data
  private val partialPageLength = getPartialPageLength(splitEnd)
  if (partialPageLength != 0) {
    splitEnd -= partialPageLength
    log.info(s"Shrunk $partialPageLength bytes.")
  }

  // align splitStart to pages and seek (only if this block is after meta/mix page)
  if (splitStart > 0) {
    val extraPageLength = sasFileReader.getSasFileProperties.getPageLength - getPartialPageLength(splitStart)
    splitStart -= extraPageLength
    if (extraPageLength != 0) {
      log.info(s"Looked back $extraPageLength bytes.")
    }

    fileInputStream.seek(splitStart)
    log.info(s"Skipped $splitStart bytes.")
    //reset progress
    countingInputStream.resetByteCount()
    //if we seek then we need to look at the current page
    //this is safe because [[currentRowOnPageIndex]] should be zero at this point
    sasFileReader.readNextPage()
  }

  override def close() {
    log.info(s"Read $getPos bytes and $recordCount records ($lastPageBlockCounter/${sasFileReader.currentPageBlockCount} on last page).")
    if (countingInputStream != null) {
      countingInputStream.close()
    }
  }

  override def createKey: NullWritable = {
    NullWritable.get
  }

  override def createValue: Array[Object] = {
    new Array[Object](sasFileReader.getSasFileProperties.getColumnsCount.toInt)
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        (getPos / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  private def getPartialPageLength(pos: Long) = (pos - sasFileReader.getSasFileProperties.getHeaderLength) % sasFileReader.getSasFileProperties.getPageLength

  override def next(key: NullWritable, value: Array[Object]): Boolean = {
    lazy val readNext = {
      val recordValue = sasFileReader.readNext()
      if (recordValue == null) {
        false
      } else {
        recordValue.copyToArray(value)
        recordCount += 1
        true
      }
    }

    // read a record if the currentPosition is less than the split end
    if (getPos < splitEnd - splitStart) {
      readNext
    } else if (
    // be especially careful about last page
      splitStart + getPos >= maxPagePosition &&
        // but don't get stuck on end of the file
        !(sasFileReader.currentPageType == ParsoWrapper.PAGE_META_TYPE &&
          sasFileReader.currentPageDataSubheaderPointers.isEmpty)
    ) {
      if (lastPageBlockCounter < sasFileReader.currentPageBlockCount) {
        lastPageBlockCounter += 1
        readNext
      } else {
        false
      }
    } else {
      false
    }
  }

  override def getPos: Long = countingInputStream.getByteCount
}
