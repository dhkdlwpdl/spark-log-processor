package com.example.spark

import com.example.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}


class LogProcessor(spark: SparkSession) {

  def process(config: Config): Unit = {
    val df = readData(config.inputCsvFile)
    val processedDF = transformData(df)
    saveData(processedDF, config)
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
    val writeData = df.write
      .format("parquet")
      .option("compression", "snappy")
      .option("path", s"${config.outputPath}/${config.targetTableName}") // TODO: 테이블이 이미 존제하는데 outputPath를 잘못 입력할 경우 고려 필요
      .partitionBy("event_date_kst")

    if (!spark.catalog.tableExists(config.targetTableName)) {
      writeData.saveAsTable(config.targetTableName) // 테이블이 없으면 생성 후 저장
    } else {
      writeData.mode("append").save() // 테이블이 있으면 append 모드로 저장
    }
  }
}