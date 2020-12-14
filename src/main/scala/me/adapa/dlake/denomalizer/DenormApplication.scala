package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.metadata.MetaDataFactory

import scala.io.Source

object DenormApplication {
  def main(args:Array[String]): Unit ={

    // Read Application configuration
    //    val appConf:Config = ConfigFactory.load("denormjob.json");
    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjob.json"
    val rawjsonString:String = Source.fromFile(filePath).getLines().mkString("\n")
    val jobMetadata = MetaDataFactory.getDenormMetadata(rawjsonString);

      // Reading Config from environment

      //    val loadService = LoadService(jobMetadata,envSparkConf)
      //
      //    loadService.execute()

      System.exit(0)
  }
}
