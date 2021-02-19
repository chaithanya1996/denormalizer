package me.adapa.dlake.denomalizer.config

import me.adapa.dlake.denomalizer.entities.JoinPathInfo

object TableMapperConfig {

  def getRelatedTables(sourceTable:String): List[String] ={
    sourceTable.toLowerCase match {
      case "fact_workhistory" => List("DIM_FAILURE_CODE",
        "CORPORATION_HIERARCHY",
        "DIM_WH_BREAKDOWN",
        "DIM_WH_DETECTION_METHOD",
        "DIM_WH_EVENT_TYPE",
        "DIM_WH_PRIORITY",
        "DIM_EQ_CRITICALITY",
        "DIM_EQ_MFR",
        "EQ_MODEL_NO",
        "TAXONOMY_HIERARCHY")
      case _ => List[String]()
    }
  }

  // Ideally This should be replcaed with DB table
  def getPartitionColName(sourceTable:String): String ={
    sourceTable.toUpperCase match {
      case "FACT_WORKHISTORY" => "WH_START_DATE_ST"
      case "FACT_EQ_AVAILABILITY" => "MONTH"
      case "USER_MAPPING" => "_master_"
      case "DIM_WH_EVENT_TYPE" => "_master_"
      case "DIM_WH_DETECTION_METHOD" => "_master_"
      case "DIM_WH_BREAKDOWN" => "_master_"
      case "DIM_UNIT_TYPE" => "_master_"
      case "DIM_FL" => "_master_"
      case "DIM_FAILURE_CODE" => "_master_"
      case "DIM_EQ_MFR" => "_master_"
      case "DIM_EQ_CRITICALITY" => "_master_"
      case "DIM_DATE" => "_master_"
      case "DIM_DATA_SUBMISSION" => "_master_"
      case "CORPORATION_HIERARCHY" => "_master_"
      case "BAD_ACTORS_FAVORITES" => "_master_"
      case "DIM_WH_PRIORITY" => "_master_"
      case "EQ_MODEL_NO" => "_master_"
      case "EQUIPMENT_NBR_DAYS" => "_master_"
      case "FACT_EQ_DOWNTIME_EVENTS" => "_master_"
      case "FACT_EQUIPMENT" => "_master_"
      case "GROUPED_BY" => "_master_"
      case "OEM_CORPORATION_HIERARCHY" => "_master_"
      case "SchemaVersions" => "_master_"
      case "systranschemas" => "_master_"
      case "TAXONOMY_HIERARCHY" => "_master_"
      case "TENANT_MAPPING" => "_master_"
      case "UNIT_UNIT_TYPE" => "_master_"
    }
  }

  def getPartitionColNameDb(sourceTable:String): String ={
    "NOT IMPLEMENTED"
  }

}
