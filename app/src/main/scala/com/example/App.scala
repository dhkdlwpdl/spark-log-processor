/*
 * This source file was generated by the Gradle 'init' task
 */
package com.example
import com.example.spark.LogProcessor
import org.apache.spark.sql.{SparkSession, functions => F}

object App {
  def main(args: Array[String]): Unit = {
    // Original CSV 파일 경로
    val inputCsvPath = "tmp/original_data/2019-Nov-Sample.csv"
    // 결과 Parquet 파일 저장 경로

    val checkpointPath = "tmp/checkpoint"
    val targetTableName = "test_table3"
    val outputParquetPath = f"file:///Users/admin/Desktop/homework/spark-log-processor/tmp/processed_data/$targetTableName"

    val spark = SparkSession.builder()
      .appName("LogProcessor")
      .config("spark.sql.shuffle.partitions", "2")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    // 체크포인트 디렉토리 설정 (장애 발생 시 복구를 위한 디렉토리)
    spark.sparkContext.setCheckpointDir(checkpointPath)

    val logProcessor = new LogProcessor(spark)

    val df = logProcessor.readCSV(inputCsvPath)
    // 체크포인트 설정 (중간 상태를 디스크에 저장)
    df.checkpoint()

    val finalRowCnt = logProcessor.writeToTable(df, outputParquetPath, targetTableName)

    print(df.count(), finalRowCnt)

    spark.stop()
  }

}