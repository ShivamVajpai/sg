package com.sg.interview.BooleanChange.context

import com.sg.interview.BooleanChange.Mnemonic
import com.sg.interview.BooleanChange.config.ConfigManager
import com.sg.interview.BooleanChange.processor.MnemonicDataProcessor
import com.sg.interview.BooleanChange.reader.MnemonicDataReader
import com.sg.interview.BooleanChange.writer.MnemonicOutputDataWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object ProductionContextManager {
  var currentProdContext: ProductionContextManager = _
  def apply(spark: SparkSession,mnemonicDataReader:MnemonicDataReader,mnemonicDataProcessor: MnemonicDataProcessor,mnemonicOutputDataWriter: MnemonicOutputDataWriter, configManager: ConfigManager): ProductionContextManager = {
    val prodContext: ProductionContextManager = new ProductionContextManager()
    prodContext.spark = spark
    prodContext.mnemonicDataReader = mnemonicDataReader
    prodContext.mnemonicDataProcessor = mnemonicDataProcessor
    prodContext.mnemonicOutputDataWriter = mnemonicOutputDataWriter
    prodContext.configManager = configManager
    currentProdContext = prodContext
    prodContext
  }
}

class ProductionContextManager extends GenericContextManager {
  override def getCurrentContext: ContextManager = ProductionContextManager.currentProdContext
}
