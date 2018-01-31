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

package com.github.saurfang.sas.parso

import java.io.InputStream
import java.util

import com.epam.parso.SasFileProperties
import com.epam.parso.impl.SasFileParser
import com.github.saurfang.sas.util.PrivateMethodExposer

object ParsoWrapper {

  def createSasFileParser(is: InputStream): SasFileParserWrapper = {
    val builderClass = Class.forName("com.epam.parso.impl.SasFileParser$Builder")
    val builderConstructor = builderClass.getDeclaredConstructors()(0)
    builderConstructor.setAccessible(true)
    val builderInstance = builderConstructor.newInstance()

    val builderExposer = PrivateMethodExposer(builderInstance.asInstanceOf[AnyRef])

    builderExposer('sasFileStream)(is)
    val sasFileParser = builderExposer('build)()

    new SasFileParserWrapper(sasFileParser.asInstanceOf[SasFileParser])
  }

  private val sasFileConstantsClass = Class.forName("com.epam.parso.impl.SasFileConstants")

  lazy val PAGE_META_TYPE: Int = {
    val field = sasFileConstantsClass.getDeclaredField("PAGE_META_TYPE")
    field.setAccessible(true)
    field.getInt(null)
  }

  lazy val DATE_TIME_FORMAT_STRINGS: util.Set[String] = {
    val field = sasFileConstantsClass.getDeclaredField("DATE_TIME_FORMAT_STRINGS")
    field.setAccessible(true)
    field.get(null).asInstanceOf[util.Set[String]]
  }

  lazy val DATE_FORMAT_STRINGS: util.Set[String] = {
    val field = sasFileConstantsClass.getDeclaredField("DATE_FORMAT_STRINGS")
    field.setAccessible(true)
    field.get(null).asInstanceOf[util.Set[String]]
  }

  lazy val EPSILON: Double = {
    val field = sasFileConstantsClass.getDeclaredField("EPSILON")
    field.setAccessible(true)
    field.getDouble(null)
  }

}

class SasFileParserWrapper(val sasFileParser: SasFileParser) {

  private[this] val sasFileReaderPrivateExposer = PrivateMethodExposer(sasFileParser)

  def getSasFileProperties: SasFileProperties = sasFileReaderPrivateExposer('getSasFileProperties)().asInstanceOf[SasFileProperties]

  def currentPageBlockCount: Int = sasFileReaderPrivateExposer.get[Int]('currentPageBlockCount)

  def readNextPage(): Unit = sasFileReaderPrivateExposer('readNextPage)()

  def readNext(): Array[Object] = sasFileReaderPrivateExposer('readNext)().asInstanceOf[Array[Object]]

  def currentPageType: Int = sasFileReaderPrivateExposer.get[Int]('currentPageType)

  def currentPageDataSubheaderPointers: java.util.List[_] = sasFileReaderPrivateExposer.
    get[java.util.List[_]]('currentPageDataSubheaderPointers)
}

