package com.sg.interview.BooleanChange.writer

import org.apache.spark.sql.{Dataset, Row}

class ConsoleMnemonicOutputDataWriter extends MnemonicOutputDataWriter {

  def writeProcessedData(outputDF: Dataset[Row]): Boolean = {
    outputDF.collect().foreach(println)
    true;
  }
}
