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

package org.apache.spark

import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  var conf = new SparkConf(false)
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  override def beforeAll() {
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.driver.memory", "3g")
    conf.set("spark.executor.memory", "3g")
    conf.set("spark.driver.maxResultSize", "2g")
    conf.set("spark.testing.memory", "2147480000")
    conf.set("spark.network.timeout", "600s")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.sql.parquet.compression.codec", "gzip")
    _sc = new SparkContext("local[4]", "test", conf)
    super.beforeAll()
  }

  override def afterAll() {
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }
}
