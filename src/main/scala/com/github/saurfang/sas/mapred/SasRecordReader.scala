package com.github.saurfang.sas.mapred

import java.io.IOException

import com.epam.parso.SasFileProperties
import com.epam.parso.impl.SasFileParser
import com.epam.parso.impl.SasFileParser.SasFileConstants
import com.github.saurfang.sas.util.PrivateMethodExposer
import org.apache.commons.io.input.CountingInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred.{FileSplit, InputSplit, RecordReader}
import org.apache.log4j.LogManager

/**
 * An [[RecordReader]] for [[SasInputFormat]]. Each split is aligned to page sized specified in the .sas7bdat meta info.
 * Each split looks back previous split if the start of split is incomplete.
 */
class SasRecordReader() extends RecordReader[NullWritable, Array[Object]] {
  private lazy val sasFileProperties: SasFileProperties = sasFileReaderPrivateExposer('getSasFileProperties)().asInstanceOf[SasFileProperties]
  private lazy val maxPagePosition: Long = sasFileProperties.getHeaderLength + sasFileProperties.getPageCount * sasFileProperties.getPageLength
  private lazy val lastPageBlockCount: Int = sasFileReaderPrivateExposer.get[Int]('currentPageBlockCount)
  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var fileInputStream: FSDataInputStream = null
  private var countingInputStream: CountingInputStream = null
  private var sasFileReader: SasFileParser = null
  private var sasFileReaderPrivateExposer: PrivateMethodExposer = null
  private var recordCount: Int = 0
  private var lastPageBlockCounter: Int = 0
  private var recordValue: Array[Object] = null

  @transient lazy val log = LogManager.getLogger(this.getClass.getName)

  override def close() {
    log.info(s"Read $getPos bytes and $recordCount records ($lastPageBlockCounter/$lastPageBlockCount on last page).")
    if (countingInputStream != null) {
      countingInputStream.close()
    }
  }

  override def createKey: NullWritable = {
    NullWritable.get
  }

  override def createValue: Array[Object] = {
    new Array[Object](sasFileProperties.getColumnsCount.toInt)
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        (getPos / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  def this(job: Configuration, split: InputSplit) {
    this()

    // the file input
    val fileSplit = split.asInstanceOf[FileSplit]

    // the byte position this fileSplit starts at
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = splitStart + fileSplit.getLength

    // the actual file we will be reading from
    val file = fileSplit.getPath
    // check compression
    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (codec != null) {
      throw new IOException("SASRecordReader does not support reading compressed files")
    }
    // get the filesystem
    val fs = file.getFileSystem(job)
    // open the File
    fileInputStream = fs.open(file)
    countingInputStream = new CountingInputStream(fileInputStream)
    // open SAS file reader to read meta data
    sasFileReader = new SasFileParser.ExternalBuilder().sasFileStream(countingInputStream).build() // new SasFileReader(countingInputStream)
    sasFileReaderPrivateExposer = PrivateMethodExposer(sasFileReader)

    log.info(sasFileProperties.toString)

    // align splitEnd to pages
    // only shrink splitEnd if this is the first split or the split starts after meta data
    val partialPageLength = getPartialPageLength(splitEnd)
    if (partialPageLength != 0) {
      splitEnd -= partialPageLength
      log.info(s"Shrunk $partialPageLength bytes.")
    }

    // align splitStart to pages and seek (only if this block is after meta/mix page)
    if (splitStart > 0) {
      val extraPageLength = sasFileProperties.getPageLength - getPartialPageLength(splitStart)
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
      sasFileReaderPrivateExposer('readNextPage)()
    }
  }

  private def getPartialPageLength(pos: Long) = (pos - sasFileProperties.getHeaderLength) % sasFileProperties.getPageLength

  override def next(key: NullWritable, value: Array[Object]): Boolean = {
    lazy val readNext = {
      val recordValue = sasFileReaderPrivateExposer('readNext)().asInstanceOf[Array[Object]]
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
        !(sasFileReaderPrivateExposer.get[Int]('currentPageType) == SasFileConstants.PAGE_META_TYPE &&
          sasFileReaderPrivateExposer.get[java.util.List[_]]('currentPageDataSubheaderPointers).size == 0)
    ) {
      if (lastPageBlockCounter < lastPageBlockCount) {
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
