package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.JobMetadata


import scala.io.Source

object Application {
  def main(args:Array[String]): Unit ={

    // Read Application configuration
//    val appConf:Config = ConfigFactory.load("denormjob.json");
    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjob.json"
    val rawjsonString:String = Source.fromFile(filePath).getLines().mkString("\n")
    val jobMetadata = JobMetadata(rawjsonString);
    // Reading Config from environment


//    val loadService = LoadService(jobMetadata,envSparkConf)
//
//    loadService.execute()

//    val ColumnNames = factWorkHistory.columns
//    val LowerCaseColumnNames = ColumnNames.map(x => x.toLowerCase)
//    val columnNamesZipped = ColumnNames.zip(LowerCaseColumnNames).map(x => col(x._1).as(x._2))
//    val factWhRenamed = factWorkHistory.select(columnNamesZipped:_*)
//
//    factWhRenamed.show()
//    factWhRenamed.printSchema()

//    factWhRenamed.write.format("org.apache.spark.sql.cassandra").option("keyspace","replica_tables").option("table","fact_workhistory").mode(SaveMode.Append).save()
//    factWorkHistory.write.format("json").save("/home/chaithanya/Documents/AssetAnswers/fact_work_history.json")
//
    System.exit(0)
  }
}
