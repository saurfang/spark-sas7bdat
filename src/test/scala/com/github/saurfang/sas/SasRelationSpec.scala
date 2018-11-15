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

import com.github.saurfang.sas.parso.ParsoWrapper
import com.github.saurfang.sas.spark._
    
import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{FlatSpec, Matchers}
import org.scalactic.TolerantNumerics.tolerantDoubleEquality

class SasRelationSpec extends FlatSpec with Matchers with SharedSparkContext{
  
  val BLOCK_SIZE = 3 * 1024 * 1024

  "SASReltion" should "read basic numeric SAS data exactly correct" in {
    
    val sqlContext = new SQLContext(sc)
    implicit val dblEquality = tolerantDoubleEquality(ParsoWrapper.EPSILON)
    
    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration
    conf.setInt("fs.local.block.size", BLOCK_SIZE)
    conf.setInt("mapred.min.split.size", BLOCK_SIZE)

    // Get the path for our test files.
    val sasRandomPath = getClass.getResource("/random.sas7bdat").getPath
    val csvRandomPath = getClass.getResource("/random.csv").getPath
    
    // Read SAS file using implicit reader.
    val sasRandomDF = sqlContext.sasFile(sasRandomPath).cache()
    
    // Print the automatically inferred schema.
    sasRandomDF.printSchema()

    // Ensure that we read the correct number of rows.
    sasRandomDF.count() should === (1000000)

    // Read CSV file.
    val csvRandomDF = sqlContext.read.format("csv").option("header", "true").load(csvRandomPath).cache()
    
    // Ensure we read the sas file correctly.
    sasRandomDF.collect().zip(csvRandomDF.collect()).foreach {
      case (row1, row2) =>
        row1.getDouble(0) should === (row2.getString(0).toDouble)
        row1.getDouble(1).toLong should === (row2.getString(1).toLong)
    }
  }

  "SASRelation" should "export datetime SAS data to csv/parquet" in {
    
    val sqlContext = new SQLContext(sc)

    // Get the path for our test file.
    val datetimeFilePath = getClass.getResource("/datetime.sas7bdat").getPath
    
    // Read SAS file using implicit reader.
    val datetimeDF = sqlContext.sasFile(datetimeFilePath).cache()

    // Print the automatically inferred schema.
    datetimeDF.printSchema()

    // Test writing SAS data to parquet.
    datetimeDF.write.mode("overwrite").format("parquet").save(getClass.getResource("/").getPath + "datetime.parquet")
    
    // Test writing SAS data to CSV.
    datetimeDF.write.mode("overwrite").format("csv").option("header", "true").save(getClass.getResource("/").getPath + "datetime.csv")
  }
  
  "SASReltion" should "read multi-typed SAS files exactly correct" in {
    
    val sqlContext = new SQLContext(sc)
    
    // Get the path for our test files.
    val sasUncompressedPath = getClass.getResource("/recapture_test_uncompressed.sas7bdat").getPath
    val parquetPath = getClass.getResource("/recapture_test.parquet").getPath
    
    // Read files
    val sasUncompressedDF = sqlContext.sasFile(sasUncompressedPath)
    val parquetDF = sqlContext.read.parquet(parquetPath)
    
    // Print the automatically inferred schema.
    sasUncompressedDF.printSchema()
    parquetDF.printSchema()
    
    // Ensure the same data was read.
    sasUncompressedDF.collect() should === (parquetDF.collect())
  }
  
  "SASReltion" should "read SAS files the same regardless of internal compression" in {
    
    val sqlContext = new SQLContext(sc)
    
    // Get the path for our test files.
    val sasCompressedPath = getClass.getResource("/recapture_test_compressed.sas7bdat").getPath
    val sasUncompressedPath = getClass.getResource("/recapture_test_uncompressed.sas7bdat").getPath
    
    // Read SAS files using implicit reader.
    val sasCompressedDF = sqlContext.sasFile(sasCompressedPath)
    val sasUncompressedDF = sqlContext.sasFile(sasUncompressedPath)
    
    // Print the automatically inferred schema.
    sasCompressedDF.printSchema()
    sasUncompressedDF.printSchema()
    
    // Ensure the same data was read.
    sasCompressedDF.collect() should === (sasUncompressedDF.collect())
  }
  
  "SASReltion" should "read SAS files with formatted numeric columns as decimals if readFormattedColsAsDecimal is enabled" in {
    
    val sqlContext = new SQLContext(sc)
    
    // Get the path for our test files.
    val sasSchoolPath = getClass.getResource("/ag121a_supp.sas7bdat").getPath
    val parquetSchoolDecimalPath = getClass.getResource("/ag121a_supp.parquet").getPath
    
    // Read files
    val sasSchoolDecimalDF = {
      sqlContext.read
        .format("com.github.saurfang.sas.spark")
        .option("readFormattedColsAsDecimal", true)
        .load(sasSchoolPath)
    }
    val parquetSchoolDecimalDF = sqlContext.read.parquet(parquetSchoolDecimalPath)
    
    // Print the automatically inferred schema.
    sasSchoolDecimalDF.printSchema()
    parquetSchoolDecimalDF.printSchema()
    
    // Ensure the same data was read.
    sasSchoolDecimalDF.collect() should === (parquetSchoolDecimalDF.collect())
  }
}
