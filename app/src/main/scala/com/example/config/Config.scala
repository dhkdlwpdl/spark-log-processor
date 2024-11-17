package com.example.config

import java.util.Properties
import java.io.FileInputStream
import java.io.File
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.example.config.ConfigUtils.loadFromProperties

case class Config(inputCsvFile: String, targetTableName: String, outputPath: String,
                  maxRetries: Int)

object ConfigUtils {
  // properties 파일을 읽어 Config 객체를 반환하는 메서드
  // TODO: 예외처리
  def loadFromProperties(filePath: String): Properties = {
    val properties = new Properties()
    val fileInputStream = new FileInputStream(filePath)

    properties.load(fileInputStream)
    properties
  }

  def loadConfigFromProperties(filePath: String): Config = {
    val properties = loadFromProperties(filePath)
    Config(
      inputCsvFile = properties.getProperty("inputCsvFile"),
      targetTableName = properties.getProperty("targetTableName"),
      outputPath = properties.getProperty("outputPath"),
      maxRetries = properties.getProperty("maxRetries", "5").toInt
    )
  }

  // .aws/credentials 파일에서 AWS 자격 증명을 로드하는 메서드
  def loadAwsCredentials(filePath: String): (String, String) = {
    val properties = loadFromProperties(filePath)

    val awsAccessKey = properties.getProperty("aws_access_key_id")
    val awsSecretKey = properties.getProperty("aws_secret_access_key")

    (awsAccessKey, awsSecretKey)
  }
}