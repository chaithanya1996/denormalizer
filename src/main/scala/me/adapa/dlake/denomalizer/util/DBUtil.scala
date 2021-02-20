package me.adapa.dlake.denomalizer.util

import com.typesafe.config.Config
import me.adapa.dlake.denomalizer.config.{BadTableConfigException, BaseTableNotConfiguredInDB}
import me.adapa.dlake.denomalizer.entities.{JoinPathInfo, TableConfig, TableConfigRecord}

import java.sql.DriverManager
import scala.collection.mutable


object DBUtil {

  def parseSelectColumns(pgColArray:Array[String]):List[List[String]] = {
      pgColArray.map(_.trim.split(",").toList).toList
  }

  @throws[BadTableConfigException]
  def parseTableJoinCols(string: String):mutable.Map[String,String] = {
    val rawStringMaps = string.trim.split(",").map(x => x.trim.split("->"))
    val filteredRawMaps = for (arrMapSingle <- rawStringMaps if arrMapSingle.size == 2 ) yield arrMapSingle
    var mappedCols = mutable.HashMap[String, String]()
    filteredRawMaps.foreach(x => mappedCols+= (x(0)->x(1)))
    return mappedCols
  }


  @throws[BadTableConfigException]
  def parseTableJoinCols2(string: String):(List[String],List[String]) = {
    val rawStringMaps = string.trim.split(",").map(x => x.trim.split("->"))
    val filteredRawMaps = for (arrMapSingle <- rawStringMaps if arrMapSingle.size == 2 ) yield arrMapSingle
    (filteredRawMaps.map(x => x(0)).toList,filteredRawMaps.map(x => x(0)).toList)
  }


  @Deprecated
  @throws[BaseTableNotConfiguredInDB]
  def getAppJoinforTable(basetableName:String,adminConfig:Config):JoinPathInfo = {
    val jdbcQuery =
      s"SELECT base_table, join_table_list, join_on_col_list, table_column_list FROM public.${adminConfig.getString("admin.join_path_table")} where base_table='${basetableName}';"

      val jCon = DriverManager.getConnection(
        s"jdbc:postgresql://${adminConfig.getString("db.hostname")}:${adminConfig.getString("db.port")}/${adminConfig.getString("db.db")}",
        adminConfig.getString("db.username"),
        adminConfig.getString("db.password"))

      val preparedSt = jCon.createStatement
      val rs = preparedSt.executeQuery(jdbcQuery)

    var joinPathInfo:JoinPathInfo = null;

   while(rs.next()){
      val colList = if(rs.getString("table_column_list").isEmpty) List[String]() else rs.getString("table_column_list").trim.split(",").toList ;
      joinPathInfo = JoinPathInfo(rs.getString("base_table"),
          rs.getString("join_table_list").trim.split(",").toList,
          rs.getString("join_on_col_list").trim.split(",").toList,
        colList)
      }
    joinPathInfo
  }

  @Deprecated
  @throws[BaseTableNotConfiguredInDB]
  def getAppJoinforTable2(basetableName:String,adminConfig:Config):TableConfigRecord  = {
    val jdbcQuery =
      s"SELECT base_table, join_table_list, table_column_list FROM public.${adminConfig.getString("admin.join_path_table")} where base_table='${basetableName}';"

    val jCon = DriverManager.getConnection(
      s"jdbc:postgresql://${adminConfig.getString("db.hostname")}:${adminConfig.getString("db.port")}/${adminConfig.getString("db.db")}",
      adminConfig.getString("db.username"),
      adminConfig.getString("db.password"))

    val preparedSt = jCon.createStatement
    val rs = preparedSt.executeQuery(jdbcQuery)

    var tableConfigRecord:TableConfigRecord = null;

    while(rs.next()){
      tableConfigRecord = TableConfigRecord(rs.getString("base_table"),
        parseTableJoinCols(rs.getString("join_table_list")),
        parseSelectColumns(rs.getArray("table_column_list").getArray.asInstanceOf[Array[String]]))
    }
    tableConfigRecord
  }

  @throws[BaseTableNotConfiguredInDB]
  def getAppJoinforTable3(basetableName:String,adminConfig:Config):TableConfig  = {
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
        parseSelectColumns(rs.getArray("table_column_list").getArray.asInstanceOf[Array[String]]))
    }
    tableConfigRecord
  }
}
