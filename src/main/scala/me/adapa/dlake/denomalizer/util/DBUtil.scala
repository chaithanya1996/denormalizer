package me.adapa.dlake.denomalizer.util

import com.typesafe.config.Config
import me.adapa.dlake.denomalizer.config.{BadTableConfigException, BaseTableNotConfiguredInDB}
import me.adapa.dlake.denomalizer.entities.TableConfig

import java.sql.DriverManager
import scala.collection.mutable


object DBUtil {

  def parseSelectColumns(pgColArray:Array[String]):List[List[String]] = {
      pgColArray.map(_.trim.split(":").toList).toList
  }

  @throws[BadTableConfigException]
  def parseTableJoinCols2(string: String):(List[String],List[String]) = {
    val rawStringMaps = string.trim.split(",").map(x => x.trim.split("->"))
    val filteredRawMaps = for (arrMapSingle <- rawStringMaps if arrMapSingle.size == 2 ) yield arrMapSingle
    (filteredRawMaps.map(x => x(0)).toList,filteredRawMaps.map(x => x(1)).toList)
  }


  @throws[BaseTableNotConfiguredInDB]
  def getAppJoinforTable(basetableName:String, adminConfig:Config):TableConfig  = {
    val jdbcQuery =
      s"SELECT base_table, join_table_list, table_column_list FROM public.${adminConfig.getString("admin.join_path_table")} where base_table='${basetableName}';"

    val jCon = DriverManager.getConnection(
      s"jdbc:postgresql://${adminConfig.getString("db.hostname")}:${adminConfig.getString("db.port")}/${adminConfig.getString("db.db")}",
      adminConfig.getString("db.username"),
      adminConfig.getString("db.password"))

    val preparedSt = jCon.createStatement
    val rs = preparedSt.executeQuery(jdbcQuery)

    var tableConfigRecord:TableConfig = null;

    while(rs.next()){
      val tablesAndColsTuple = parseTableJoinCols2(rs.getString("join_table_list"))
      tableConfigRecord = TableConfig(rs.getString("base_table"),
        tablesAndColsTuple._1,tablesAndColsTuple._2,
        rs.getArray("table_column_list").getArray.asInstanceOf[Array[String]].toList)
    }
    tableConfigRecord
  }
}
