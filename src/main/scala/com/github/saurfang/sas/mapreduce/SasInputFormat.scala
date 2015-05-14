package com.github.saurfang.sas.mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

/**
 * SasInputFormat is an input format used to read .sas7bdat input files, the
 * binary SAS data file. The file cannot be compressed on the filesystem level
 * otherwise the file is not splittable.
 */
class SasInputFormat extends FileInputFormat[NullWritable, Array[Object]] {
  override def createRecordReader(
                                   inputSplit: InputSplit,
                                   taskAttemptContext: TaskAttemptContext
                                   ): RecordReader[NullWritable, Array[Object]] = {
    new SasRecordReader()
  }

  override def isSplitable(context: JobContext, file: Path): Boolean = {
    new CompressionCodecFactory(context.getConfiguration).getCodec(file) == null
  }
}