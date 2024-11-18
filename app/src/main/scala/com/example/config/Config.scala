package com.example.config

import java.util.Properties
import java.io.FileInputStream


case class Config(inputCsvFile: String, targetTableName: String, outputPath: String,
                  maxRetries: Int)
case class AwsCredentials(awsAccessKey: String, awsSecretKey: String)

object ConfigUtils {
  /**
   * 애플리케이션 설정 정보를 로드하여 Config 객체로 반환
   * @param filePath 설정 파일(conf/conf.properties)의 경로
   * @return 애플리케이션 설정 값을 포함하는 Config 객체
   */
  def loadConfigFromProperties(filePath: String): Config = {
    try {
      val properties = loadFromProperties(filePath)
      Config(
        inputCsvFile = properties.getProperty("inputCsvFile"),
        targetTableName = properties.getProperty("targetTableName"),
        outputPath = properties.getProperty("outputPath"),
        maxRetries = properties.getProperty("maxRetries", "5").toInt
      )
    } catch {
      case e: Exception => throw new RuntimeException(s"Failed to load configuration from $filePath: $e")
    }

  }

  /**
   * AWS 자격 증명 정보를 로드하여 AwsCredentials 객체로 반환
   * 로컬 개발 환경에서만 사용되며, Spark 프로덕션 환경에서는 ~/.aws/credentials 파일을 자동으로 로드
   * @param filePath AWS 자격 증명 파일(conf/credentials)의 경로
   * @return AWS 자격 증명 정보를 포함하는 AwsCredentials 객체
   */
  def loadAwsCredentials(filePath: String): AwsCredentials = {
    try {
      val properties = loadFromProperties(filePath)

      val awsAccessKey = properties.getProperty("aws_access_key_id")
      val awsSecretKey = properties.getProperty("aws_secret_access_key")

      AwsCredentials(awsAccessKey, awsSecretKey)
    } catch {
      case e: Exception => throw new RuntimeException(s"Failed to load configuration from $filePath: $e")
    }
  }

  /**
   * 주어진 경로의 프로퍼티 파일을 로드하여 Properties 객체로 반환
   * @param filePath 프로퍼티 파일의 경로
   * @return 로드된 Properties 객체
   */
  private def loadFromProperties(filePath: String): Properties = {
    val properties = new Properties()
    val fileInputStream = new FileInputStream(filePath)

    properties.load(fileInputStream)
    properties
  }
}