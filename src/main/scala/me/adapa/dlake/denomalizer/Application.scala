package me.adapa.dlake.denomalizer

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.typesafe.config.{Config, ConfigFactory}
import me.adapa.dlake.denomalizer.entities.JobMetadata
import me.adapa.dlake.denomalizer.executioner.DenormalizerService

object Application {
  def main(args:Array[String]): Unit ={

    // Read Application configuration
    val appConf:Config = ConfigFactory.load("denormjob.conf");

    val jobMetadata = JobMetadata(appConf);

    println(jobMetadata)
    // Reading Config from environment
    val envConf:Config = ConfigFactory.load()

    val envSparkConf = new SparkConf(true).set("spark.cassandra.connection.host", envConf.getString("cassandra.host"))
      .set("spark.cassandra.auth.username", envConf.getString("cassandra.username"))
      .set("spark.cassandra.auth.password", envConf.getString("cassandra.password"))
      .set("fs.s3a.connection.ssl.enabled",value = "false")
      .set("fs.s3a.endpoint",envConf.getString("s3.endpoint"))
      .set("fs.s3a.access.key",envConf.getString("s3.accessKey"))
      .set("fs.s3a.secret.key",envConf.getString("s3.secretKey"))

//    println(envSparkConf)


    
//    val spark = SparkSession.builder.config(envSparkConf)
//      .master("local[*]")
//      .appName("Denormalizer Application")
//      .getOrCreate()

    val denormService = DenormalizerService(jobMetadata,envSparkConf)
    denormService.execute()
//    val factWorkHistory = spark.read
//      .format("delta")
//      .option("header",value = true)
//      .option("inferSchema",value = true)
//      .load(s"s3a://obj/AssetAnswers/FACT_WORKHISTORY")

//    val factWorkHistory = spark.read
//      .format("jdbc")
//      .option("header",value = true)
//      .option("inferSchema",value = true)
//      .load(s"s3a://obj/AssetAnswers/FACT_WORKHISTORY")

//    dataFrame.write
//      .option("header",value = true)
//      .format("delta")
//      .save("/home/chaithanya/Documents/FACT_WH/adapa")

//
//    val collectionC = spark.sparkContext.parallelize(Seq(emp(5, "cat","dog",0,0), emp(6, "fox","dog",0,99)))
//    val dfman  = spark.createDataFrame(collectionC)

//    dfman.write.cassandraFormat.options(Map("table" -> "emp","keyspace"->"replica_tables")).mode(SaveMode.Append).save()
//    collectionC.saveToCassandra("replica_tables","emp",SomeColumns("emp_id" , "emp_city" , "emp_name" , "emp_phone" , "emp_sal"))
//    factWorkHistory.printSchema()
//    factWorkHistory.show()
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
