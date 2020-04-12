package com.sg.interview.BooleanChange.processor

import org.apache.spark.sql.{Dataset, Row}

trait MnemonicDataProcessor {

  def processData(inputDf: Dataset[Row]): Dataset[Row]
}
