package agh.wggios.analizadanych

import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.transformer.DataTransformer
import agh.wggios.analizadanych.datawriter.DataWriter

object Main extends SparkSessionProvider {
  LoggingUtils.setupLogging()

  def main(args: Array[String]): Unit = {
    val df = new DataReader().read_csv(args(0))
    val transformed = new DataTransformer().addRowNumber(df)

    new DataWriter().writeParquet(transformed, "output/plik.parquet")
    new DataWriter().saveToCsv(transformed, "output/plik.csv")
  }
}
