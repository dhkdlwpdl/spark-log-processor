/*
 * This source file was generated by the Gradle 'init' task
 */
package com.example
import com.example.config.{Config, ConfigUtils}
import com.example.spark.LogProcessor
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks.break

object App {
  def main(args: Array[String]): Unit = {
    // 설정 파일 로딩
    val config = ConfigUtils.loadFromProperties("conf/conf.properties")

    val spark = SparkSession.builder()
      .appName("LogProcessor")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val processor = new LogProcessor(spark)

    if (processWithRetry(processor, config)) {
      println("Process Succeed !")
    } else {
      println(s"Failed after $config.maxRetries attempts.")
    }

    spark.stop()
  }

  private def processWithRetry(processor: LogProcessor, config: Config): Boolean = {
    var attempt = 0
    var success = false

    // 최대 재시도 횟수까지 반복
    while (attempt < config.maxRetries && !success) {
      attempt += 1
      println(s"Attempt #$attempt...")

      if (processor.process(config)) {
        success = true
        break
      } else {
        println(s"Attempt #$attempt failed.")
        if (attempt < config.maxRetries) {
          println(s"Retrying... ($attempt/${config.maxRetries})")
        }
      }
    }

    success
  }

}
