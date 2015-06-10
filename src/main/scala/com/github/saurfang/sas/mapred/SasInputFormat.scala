package com.github.saurfang.sas.mapred

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred._

/**
 * SasInputFormat is an input format used to read .sas7bdat input files, the
 * binary SAS data file. The file cannot be compressed on the filesystem level
 * otherwise the file is not splittable.
 */
class SasInputFormat extends FileInputFormat[NullWritable, Array[Object]] {
  override def getRecordReader(inputSplit: InputSplit, jobConf: JobConf, reporter: Reporter): RecordReader[NullWritable, Array[Object]] = {
    new SasRecordReader(jobConf, inputSplit)
  }

  override def isSplitable(fs: FileSystem, filename: Path): Boolean = {
    new CompressionCodecFactory(fs.getConf).getCodec(filename) == null
  }
}