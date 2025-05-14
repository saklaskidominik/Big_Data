package agh.wggios.analizadanych.datawriter

import org.apache.spark.sql.DataFrame

class DataWriter {
  def writeParquet(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
  }

  def saveToCsv(df: DataFrame, path: String): Unit = {
    df.write
      .option("header", "true")
      .mode("overwrite")
      .csv(path)
  }
}
