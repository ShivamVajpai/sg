package com.sg.interview.BooleanChange.writer

import org.apache.spark.sql.{Dataset, Row}

trait MnemonicOutputDataWriter {

  def writeProcessedData(outputDF: Dataset[Row]): Boolean
}
