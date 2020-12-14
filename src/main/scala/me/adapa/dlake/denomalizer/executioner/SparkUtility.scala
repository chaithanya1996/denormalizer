package me.adapa.dlake.denomalizer.executioner

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object SparkUtility {

  def ConvertDataframeColToLowerCase(inputDf:DataFrame): DataFrame ={
    val ColumnNames = inputDf.columns
    val LowerCaseColumnNames = ColumnNames.map(x => x.toLowerCase)
    val columnNamesZipped = ColumnNames.zip(LowerCaseColumnNames).map(x => col(x._1).as(x._2))
    inputDf.select(columnNamesZipped: _*)
  }

}
