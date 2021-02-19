package me.adapa.dlake.denomalizer

import me.adapa.dlake.denomalizer.entities.metadata.MetaDataFactory
import me.adapa.dlake.denomalizer.executioner.DenormalizerService

import scala.io.Source
import com.typesafe.config.{Config, ConfigFactory}
import me.adapa.dlake.denomalizer.util.DBUtil

object DenormApplication {
  def main(args:Array[String]): Unit ={

    // Administrative configuration for the APP
    val appAdminConfig:Config  = ConfigFactory.load();

    val filePath = args(0)
//    val filePath = "/home/chaithanya/Documents/Projects/denormalizer/src/main/resources/denormjob.json"
    val sourceFile = Source.fromFile(filePath)
    val rawjsonString:String = sourceFile.getLines().mkString("\n")
    val jobMetadata = MetaDataFactory.getDenormMetadata(rawjsonString);

    //Reading Config from environment
    val denormService = DenormalizerService(jobMetadata,appAdminConfig)
    denormService.execute()

      System.exit(0)
  }
}
