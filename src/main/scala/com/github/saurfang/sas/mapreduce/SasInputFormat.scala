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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

/**
  * SasInputFormat is an input format used to read .sas7bdat input files, the
  * binary SAS data file. The file cannot be compressed on the filesystem level
  * otherwise the file is not splittable.
  */
class SasInputFormat extends FileInputFormat[NullWritable, Array[Object]] {

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[NullWritable, Array[Object]] = {
    new SasRecordReader(split, context)
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    // Sas requires reading page boundaries, and SplittableCompressionCodec does not support
    // seeking, and makes InputSplits that do not contain the sas block. So for any externally
    // compressed sas file, it must be processed in a single InputSplit.
    Option(new CompressionCodecFactory(context.getConfiguration).getCodec(filename)).isEmpty
  }

  // Our splitting logic requires each internal SAS page to occur in only one split.
  // So, don't allow splits smaller than 1MB, and hope that no SAS datasets have page lengths larger than this.
  override def getFormatMinSplitSize: Long = {
    1000000
  }
}
