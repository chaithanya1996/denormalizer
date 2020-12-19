package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.metadata.MetaDataFactory

import scala.io.Source

object DenormApplication {
  def main(args:Array[String]): Unit ={

    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjob.json"
    val sourceFile = Source.fromFile(filePath)
    val rawjsonString:String = sourceFile.getLines().mkString("\n")
    val jobMetadata = MetaDataFactory.getDenormMetadata(rawjsonString);

      // Reading Config from environment

      //    val loadService = LoadService(jobMetadata,envSparkConf)
      //
      //    loadService.execute()

      System.exit(0)
  }
}
