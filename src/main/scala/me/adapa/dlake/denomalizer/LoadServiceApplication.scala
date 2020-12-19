package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.metadata.{LoadMetaData, MetaDataFactory}
import me.adapa.dlake.denomalizer.executioner.LoadService

import scala.io.Source

object LoadServiceApplication {

  def main(args:Array[String]): Unit ={

    //    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjobV2.json"
    val filePath = args(0)
    val sourceFile = Source.fromFile(filePath)
    val rawjsonString:String = sourceFile.getLines().mkString("\n")
    val jobMetadata:LoadMetaData = MetaDataFactory.getLoadMetadata(rawjsonString);

    // Reading Config from environment
    val loadService = LoadService(jobMetadata)
    loadService.execute()

    System.exit(0)
  }

}
