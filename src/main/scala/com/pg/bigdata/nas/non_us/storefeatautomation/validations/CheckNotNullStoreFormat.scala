package com.pg.bigdata.nas.non_us.storefeatautomation.validations

import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.Columns.{cnt, id, pos_cust_id, pos_store_id, retailer_name}
import com.pg.bigdata.nas.non_us.storefeatautomation.utils.colToString
import com.pg.bigdata.util.logger.UtilLogger
import com.pg.bigdata.util.validation.Validation
import com.pg.bigdata.util.validation.identifiers.{Number, ThresholdType}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

class CheckNotNullRetailerName(storeFeatDf: DataFrame) extends Validation {

  override def description: String = "Checks if retailer_name column does not contain nulls"

  override def thresholdType: ThresholdType = Number

  override def thresholdCriticalOver: Double = 0.0

  override def thresholdGoodTo: Double = 0.0

  override def badRecordsFilter: String = "true"

  override def prepareData(implicit spark: SparkSession, logger: UtilLogger): DataFrame = {
    storeFeatDf.where(retailer_name.isNull)
  }

}
