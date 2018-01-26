package com.github.saurfang.sas.parso

import org.scalatest.{FlatSpec, Matchers}

class ParsoWrapperSpec extends FlatSpec with Matchers {

  "ParsoWrapper" should "create SasFileParserWrapper" in {
    val is = getClass.getResource("/datetime.sas7bdat").openStream()

    val sasFileParserWrapper = ParsoWrapper.createSasFileParser(is)

    sasFileParserWrapper should not be null

    is.close()
  }

  "ParsoWrapper" should "return non null constants" in {
    ParsoWrapper.DATE_FORMAT_STRINGS should not be null
    ParsoWrapper.DATE_FORMAT_STRINGS should not be empty

    ParsoWrapper.DATE_TIME_FORMAT_STRINGS should not be null
    ParsoWrapper.DATE_TIME_FORMAT_STRINGS should not be empty

    noException should be thrownBy ParsoWrapper.EPSILON
    noException should be thrownBy ParsoWrapper.PAGE_META_TYPE
  }
}
