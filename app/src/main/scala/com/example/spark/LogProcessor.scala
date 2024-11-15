package com.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

class LogProcessor(spark: SparkSession) {
  def readCSV(inputCsvPath: String): DataFrame = {
    // 마지막 처리된 timestamp 추적 (예: 메타데이터에서 읽어와야 함)
    val lastProcessedTimestamp = "2023-01-01T00:00:00Z"  // 이 값을 실제 메타데이터에서 가져옵니다.

    // CSV 파일 읽기
    spark.read
      .option("header", "true")  // 첫 번째 행을 헤더로 처리
      .option("inferSchema", "true")  // 데이터의 타입을 자동으로 추론
      .csv(inputCsvPath)
    //       .filter(F.col("event_time").geq(lastProcessedTimestamp))  // timestamp 기준 필터링 (추가 기간 처리)
  }

  def writeToTable(df: DataFrame, outputParquetPath: String, targetTableName: String): Long = {
    // Parquet 형식으로 저장 (Snappy 압축)
    val writeData = df
      .withColumn("event_date", F.date_format(F.from_utc_timestamp(F.col("event_time"), "Asia/Seoul"), "yyyy-MM-dd")) // event_time을 KST로 변환하고 일자만 추출
      .write
      .format("parquet")
      .option("compression", "snappy")  // Snappy 압축
      .option("path", outputParquetPath)
      .partitionBy("event_date") // 파티셔닝 기준으로 event_date 사용

    if (!spark.catalog.tableExists(targetTableName)) {
      writeData.saveAsTable(targetTableName)  // 테이블이 없으면 생성 후 저장
    } else {
      writeData.mode("append").save()  // 테이블이 있으면 append 모드로 저장
    }

    spark.sql(f"select * from $targetTableName").count()
    // 영향을 받은 Row 개수 반환 - TODO: 이게 필요한지 고민해보기
  }
}
