package com.sg.interview.BooleanChange.context

import com.sg.interview.BooleanChange.config.ConfigManager
import com.sg.interview.BooleanChange.processor.{SparkDSLMnemonicDataProcessor, SparkSQLMnemonicDataProcessor}
import com.sg.interview.BooleanChange.reader.CSVMnemonicDataReader
import com.sg.interview.BooleanChange.writer.{CSVMnemonicOutputDataWriter, ConsoleMnemonicOutputDataWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ContextFactory {
  private var defaultContext: ContextManager = _

  def initForProdSQL(spark:SparkSession): ContextManager = {
    val mnemonicDataReader = CSVMnemonicDataReader(spark)
    val mnemonicDataProcessor = SparkSQLMnemonicDataProcessor(spark)
    val mnemonicOutputDataWriter = new ConsoleMnemonicOutputDataWriter()
    val configManager = ConfigManager(null)
    defaultContext = ProductionContextManager(spark,mnemonicDataReader,mnemonicDataProcessor,mnemonicOutputDataWriter,configManager)
    defaultContext
  }

  def initForProdSQLWithLocalSparkSession(): ContextManager = {
    val conf = new SparkConf().setMaster("local").setAppName("MnemonicViaSQL")
    val spark = org.apache.spark.sql.SparkSession.builder.config(conf).getOrCreate
    initForProdSQL(spark)
  }

  def initForProdDSL(spark:SparkSession): ContextManager = {
    val mnemonicDataReader = CSVMnemonicDataReader(spark)
    val mnemonicDataProcessor = SparkDSLMnemonicDataProcessor(spark)
    val mnemonicOutputDataWriter = new CSVMnemonicOutputDataWriter()
    val configManager = ConfigManager(null)
    defaultContext = ProductionContextManager(spark,mnemonicDataReader,mnemonicDataProcessor,mnemonicOutputDataWriter,configManager)
    defaultContext
  }

  def initForProdDSLWithLocalSparkSession(): ContextManager = {
    val conf = new SparkConf().setMaster("local").setAppName("MnemonicViaDSL").set("hadoop.home.dir","C:\\developer\\softwares\\hadoop")
    val spark = org.apache.spark.sql.SparkSession.builder.config(conf).getOrCreate
    initForProdDSL(spark)
  }


  def initForEnvironment(env: String): ContextManager = {
  // match case : based on the envrionment String return appropriate ContextManager
    null
  }
  def getCurrentContext : ContextManager = ContextFactory.defaultContext
}
