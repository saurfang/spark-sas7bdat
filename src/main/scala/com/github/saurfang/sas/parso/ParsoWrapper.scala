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

package com.github.saurfang.sas.parso

import java.io.InputStream
import java.util

import com.epam.parso.SasFileProperties
import com.epam.parso.impl.SasFileParser
import com.github.saurfang.sas.util.PrivateMethodExposer

/**
  * An object to expose private methods from com.epam.parso.impl.SasFileParser and com.epam.parso.impl.SasFileConstants.
  */
object ParsoWrapper {

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

  // Read SAS file metadata constants specified by parso.
  private val sasFileConstantsClass = Class.forName("com.epam.parso.impl.SasFileConstants")

  // Define a method to build a SasFileParserWrapper
  def createSasFileParser(inputStream: InputStream): SasFileParserWrapper = {

    // Get an instance of SasFileParser.Builder
    val builderClass = Class.forName("com.epam.parso.impl.SasFileParser$Builder")
    val builderConstructor = builderClass.getDeclaredConstructors()(0)
    builderConstructor.setAccessible(true)
    val builderInstance = builderConstructor.newInstance()

    // Get a private method exposer for the builder.
    val builderExposer = PrivateMethodExposer(builderInstance.asInstanceOf[AnyRef])

    // Build a sasFileParser from our input stream
    builderExposer('sasFileStream)(inputStream)
    val sasFileParser = builderExposer('build)()

    new SasFileParserWrapper(sasFileParser.asInstanceOf[SasFileParser])
  }
}

class SasFileParserWrapper(val sasFileParser: SasFileParser) {

  private[this] val sasFileParserPrivateExposer = PrivateMethodExposer(sasFileParser)

  // Expose getSasFileProperties()
  def getSasFileProperties(): SasFileProperties = {
    sasFileParserPrivateExposer('getSasFileProperties)().asInstanceOf[SasFileProperties]
  }

  // Expose readNext()
  def readNext(): Array[Object] = {
    sasFileParserPrivateExposer('readNext)(null).asInstanceOf[Array[Object]]
  }

  // Expose readNextPage()
  def readNextPage(): Unit = {
    sasFileParserPrivateExposer('readNextPage)()
  }
}
