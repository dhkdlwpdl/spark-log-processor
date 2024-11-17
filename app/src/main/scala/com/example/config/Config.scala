package com.example.config

import java.util.Properties
import java.io.FileInputStream

case class Config(inputCsvFile: String, targetTableName: String, outputPath: String)

object ConfigUtils {
  // properties 파일을 읽어 Config 객체를 반환하는 메서드
  // TODO: 예외처리
  def loadFromProperties(filePath: String): Config = {
    val properties = new Properties()
    val fileInputStream = new FileInputStream(filePath)

    properties.load(fileInputStream)

    Config(
      inputCsvFile = properties.getProperty("inputCsvFile"),
      targetTableName = properties.getProperty("targetTableName"),
      outputPath = properties.getProperty("outputPath")
    )
  }
}