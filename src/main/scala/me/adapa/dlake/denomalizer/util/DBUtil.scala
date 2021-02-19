package me.adapa.dlake.denomalizer.util

import com.typesafe.config.Config
import me.adapa.dlake.denomalizer.config.BaseTableNotConfiguredInDB
import me.adapa.dlake.denomalizer.entities.JoinPathInfo
import java.sql.DriverManager

object DBUtil {

  @throws[BaseTableNotConfiguredInDB]
  def getAppJoinforTable(basetableName:String,adminConfig:Config):JoinPathInfo = {
    val jdbcQuery =
      s"SELECT base_table, join_table_list, join_on_col_list, table_column_list FROM public.${adminConfig.getString("admin.join_path_table")};"

      val jCon = DriverManager.getConnection(
        s"jdbc:postgresql://${adminConfig.getString("db.hostname")}:${adminConfig.getString("db.port")}/${adminConfig.getString("db.db")}",
        adminConfig.getString("db.username"),
        adminConfig.getString("db.password"))


      val preparedSt = jCon.createStatement

      val rs = preparedSt.executeQuery(jdbcQuery)

    var joinPathInfo:JoinPathInfo = null;
    if (rs.next()){
      val colList = if(rs.getString("table_column_list").isEmpty) List[String]() else rs.getString("table_column_list").trim.split(",").toList ;
      joinPathInfo = JoinPathInfo(rs.getString("base_table"),
          rs.getString("join_table_list").trim.split(",").toList,
          rs.getString("join_on_col_list").trim.split(",").toList,
        colList)
      }else {
      rs.close()
      jCon.close()
      throw  new BaseTableNotConfiguredInDB;
    }

    joinPathInfo
  }
}
