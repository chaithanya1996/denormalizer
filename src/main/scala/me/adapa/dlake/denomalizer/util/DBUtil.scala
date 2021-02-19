package me.adapa.dlake.denomalizer.util

import com.typesafe.config.Config
import me.adapa.dlake.denomalizer.config.BaseTableNotConfiguredInDB
import me.adapa.dlake.denomalizer.entities.JoinPathInfo
import java.sql.DriverManager

object DBUtil {

  @throws[BaseTableNotConfiguredInDB]
  def getAppJoinforTable(basetableName:String,adminConfig:Config):JoinPathInfo = {
    val jdbcQuery =
      """SELECT base_table, join_table_list, join_on_col_list, table_column_list
         |FROM ? where base_table = ?""".stripMargin


      val jCon = DriverManager.getConnection(
        s"jdbc:postgresql://${adminConfig.getString("db.hostname")}:${adminConfig.getString("db.port")}/${adminConfig.getString("db.db")}",
        adminConfig.getString("db.username"),
        adminConfig.getString("db.password"))


      val preparedSt = jCon.prepareStatement(jdbcQuery)
      preparedSt.setString(1, adminConfig.getString("admin.join_path_table"))
      preparedSt.setString(2, basetableName)
      var rs = preparedSt.executeQuery

      rs.next

      val joinPathInfo:JoinPathInfo = JoinPathInfo(rs.getString("base_table"),
        rs.getArray("join_table_list").asInstanceOf[Array[String]].toList,
        rs.getArray("join_on_col_list").asInstanceOf[Array[String]].toList,
        rs.getArray("table_column_list").asInstanceOf[Array[String]].toList)

      rs.close()

    joinPathInfo
  }
}
