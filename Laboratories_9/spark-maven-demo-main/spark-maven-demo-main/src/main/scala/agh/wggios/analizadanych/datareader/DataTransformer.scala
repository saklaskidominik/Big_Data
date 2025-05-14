package agh.wggios.analizadanych.transformer

import org.apache.spark.sql.{DataFrame, functions => F}

class DataTransformer {
  def addRowNumber(df: DataFrame): DataFrame = {
    df.withColumn("row_id", F.monotonically_increasing_id())
  }
}
