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

package com.github.saurfang.sas

import com.github.saurfang.sas.mapred.SasInputFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.log4j.LogManager
import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class SasInputFormatSpec extends FlatSpec with Matchers with SharedSparkContext {
  
  @transient lazy val log = LogManager.getLogger(this.getClass.getName)
  
  val BLOCK_SIZE: Int = 3 * 1024 * 1024
  val minPartitions: Int = 100
  
  "SASInputFormat" should "read correct number of records" in {
    
    val sqlContext = new SQLContext(sc)
    
    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration
    conf.setInt("fs.local.block.size", BLOCK_SIZE)
    conf.setInt("mapred.min.split.size", BLOCK_SIZE)
    
    // Get the path for our test file.
    val sasRandomPath = getClass.getResource("/random.sas7bdat").getPath

    // Read an RDD using SASInputFormat.
    val sasRDD: RDD[(NullWritable, Array[Object])] = 
      sqlContext.sparkContext.hadoopFile(
        sasRandomPath, 
        classOf[SasInputFormat], 
        classOf[NullWritable],
        classOf[Array[Object]], 
        minPartitions)
    
    // Log the block size of our test file.
    val fileStatus = FileSystem.get(conf).getFileStatus(new Path(sasRandomPath))
    log.info(s"Block Size: ${fileStatus.getBlockSize}")
    
    // Ensure the RDD has read the correct number of records.
    sasRDD.count() should === (1000000)
  }
}
