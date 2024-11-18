package com.example.spark

import com.example.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

class LogProcessor(spark: SparkSession) {

  /**
   * 로그 데이터를 처리하는 메서드
   * @param config 애플리케이션 설정 정보를 담고 있는 Config 객체
   * @return 처리 성공 여부 (true: 성공, false: 실패)
   */
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

  /**
   * 입력 파일에서 CSV 데이터를 읽어 DataFrame으로 반환
   * @param inputPath inputPath CSV 파일 경로
   * @return Spark DataFrame
   */
  private def readData(inputPath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
  }

  /**
   * DataFrame 데이터를 변환하는 메서드
   * @param df 변환할 DataFrame
   * @return 변환된 DataFrame
   */
  private def transformData(df: DataFrame): DataFrame = {
    df.withColumn(
      "event_date_kst",
      F.date_format(F.from_utc_timestamp(F.col("event_time"), "Asia/Seoul"), "yyyy-MM-dd")
    )
  }

  /**
   * 변환된 데이터를 Delta 포맷으로 저장.
   * 테이블이 존재하지 않는 경우 생성. External Table로 관리됨.
   * @param df 저장할 DataFrame
   * @param config 애플리케이션 설정 정보를 담고 있는 Config 객체
   */
  private def saveData(df: DataFrame, config: Config): Unit = {
    val dataPath = s"${config.outputPath}/${config.targetTableName}" // 데이터 저장 경로

    // Delta 테이블 포맷으로 데이터 저장
    df.write
      .format("delta")
      .mode("append")
      .option("compression", "snappy")
      .partitionBy("event_date_kst")
      .save(dataPath)

    // 테이블이 존재하지 않으면 Delta 테이블 생성
    if (!spark.catalog.tableExists(config.targetTableName)) {
      spark.sql(s"CREATE SCHEMA IF NOT EXISTS delta LOCATION '${config.outputPath}'")
      spark.sql(s"CREATE TABLE IF NOT EXISTS delta.${config.targetTableName} USING DELTA LOCATION " +
        s"'$dataPath'")
    }
  }
}