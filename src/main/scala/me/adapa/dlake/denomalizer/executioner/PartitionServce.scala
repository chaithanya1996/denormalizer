package me.adapa.dlake.denomalizer.executioner

import org.apache.spark.sql.functions.{col, concat, month, quarter, year}
import org.apache.spark.sql.DataFrame

object PartitionServce {

  def repartitionDfMonth(inputDf:DataFrame, tsColumn:String): DataFrame ={
    return inputDf.withColumn("_partition_year", year(col(tsColumn)))
      .withColumn("_partition_month", quarter(col(tsColumn)))
      .withColumn("_partition_key",concat(col("_partition_year"),col("_partition_month")))
      .repartition(col("_partition_key"))
  }

}
