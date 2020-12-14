package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.metadata.{LoadMetaData, MetaDataFactory}
import me.adapa.dlake.denomalizer.executioner.LoadService

import scala.io.Source

object LoadServiceApplication {

  def main(args:Array[String]): Unit ={
    // Read Application configuration
    //    val appConf:Config = ConfigFactory.load("denormjob.json");
    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjob.json"
    val rawjsonString:String = Source.fromFile(filePath).getLines().mkString("\n")
    val jobMetadata:LoadMetaData = MetaDataFactory.getLoadMetadata(rawjsonString);
    // Reading Config from environment
    val loadService = LoadService(jobMetadata)
    loadService.execute()
    System.exit(0)
  }

}
