package com.sg.interview.BooleanChange.writer

import org.apache.spark.sql.{Dataset, Row}

class CSVMnemonicOutputDataWriter extends MnemonicOutputDataWriter{

  def writeProcessedData(outputDF: Dataset[Row]): Boolean = {
    outputDF.persist()
    outputDF.repartition(1).write.format("parquet").option("header", "true").mode("overwrite").save("file:///C:/developer/output/SG/sensor.csv")
    true;
  }
}
