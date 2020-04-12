package com.sg.interview.BooleanChange.reader

import org.apache.spark.sql.{Dataset, Row}

trait MnemonicDataReader {

  def readInputData(): Dataset[Row]
}
