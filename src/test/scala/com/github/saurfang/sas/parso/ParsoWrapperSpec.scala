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

import org.scalatest.{FlatSpec, Matchers}

class ParsoWrapperSpec extends FlatSpec with Matchers {

  "ParsoWrapper" should "create SasFileParserWrapper" in {

    // Get an input stream containing a SAS dataset.
    val inputStream = getClass.getResource("/datetime.sas7bdat").openStream()

    // Attempt to create a SasFileParserWrapper from this stream.
    val sasFileParserWrapper = ParsoWrapper.createSasFileParser(inputStream)

    sasFileParserWrapper should not be null
    inputStream.close()
  }

  "ParsoWrapper" should "return non null constants from parso" in {

    // Ensure our wrapper correctly extracted the private constants from Parso.
    ParsoWrapper.DATE_FORMAT_STRINGS should not be null
    ParsoWrapper.DATE_FORMAT_STRINGS should not be empty
    ParsoWrapper.DATE_TIME_FORMAT_STRINGS should not be null
    ParsoWrapper.DATE_TIME_FORMAT_STRINGS should not be empty
    noException should be thrownBy ParsoWrapper.EPSILON
  }
}
