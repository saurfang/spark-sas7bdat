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
