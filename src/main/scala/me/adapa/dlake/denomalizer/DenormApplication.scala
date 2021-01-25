package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.metadata.MetaDataFactory
import me.adapa.dlake.denomalizer.executioner.DenormalizerService

import scala.io.Source

object DenormApplication {
  def main(args:Array[String]): Unit ={

    val filePath = args(0)
//    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjob.json"
    val sourceFile = Source.fromFile(filePath)
    val rawjsonString:String = sourceFile.getLines().mkString("\n")
    val jobMetadata = MetaDataFactory.getDenormMetadata(rawjsonString);

    //Reading Config from environment
    val denormService = DenormalizerService(jobMetadata)
    denormService.execute()

      System.exit(0)
  }
}
