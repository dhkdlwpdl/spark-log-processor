package com.example.spark

import com.example.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

class LogProcessor(spark: SparkSession) {

  def process(config: Config): Boolean = {
    try {
      val df = readData(config.inputCsvFile)
      val processedDF = transformData(df)
      saveData(processedDF, config)
      true
    } catch {
      case e: Exception =>
        println(s"ERROR Processing Log Event: $e")
        false
    }
  }

  private def readData(inputPath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
  }

  private def transformData(df: DataFrame): DataFrame = {
    df.withColumn(
      "event_date_kst",
      F.date_format(F.from_utc_timestamp(F.col("event_time"), "Asia/Seoul"), "yyyy-MM-dd")
    )
  }

  private def saveData(df: DataFrame, config: Config): Unit = {
    val dataPath = s"${config.outputPath}/${config.targetTableName}"
    df.write
      .format("delta")
      .mode("append")
      .option("compression", "snappy")
      .partitionBy("event_date_kst")
      .save(dataPath) // TODO: 테이블이 이미 존제하는데 outputPath를 잘못 입력할 경우 고려 필요

    if (!spark.catalog.tableExists(config.targetTableName)) {
      spark.sql(s"CREATE SCHEMA IF NOT EXISTS delta LOCATION '${config.outputPath}'")
      spark.sql(s"CREATE TABLE IF NOT EXISTS delta.${config.targetTableName} USING DELTA LOCATION " +
        s"'$dataPath'")
    }
  }
}