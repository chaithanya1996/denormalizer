package me.adapa.dlake.denomalizer.config

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
        "FACT_EQUIPMENT",
        "DIM_EQ_CRITICALITY",
        "DIM_EQ_MFR",
        "EQ_MODEL_NO",
        "TAXONOMY_HIERARCHY",
        "FACT_EQ_DOWNTIME_EVENTS")
      case _ => List[String]()
    }
  }

}
