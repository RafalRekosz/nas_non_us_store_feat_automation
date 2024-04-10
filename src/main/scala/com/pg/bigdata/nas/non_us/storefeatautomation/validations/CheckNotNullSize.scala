package com.pg.bigdata.nas.non_us.storefeatautomation.validations

import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.Columns.{retailer_name, store_format}
import com.pg.bigdata.util.logger.UtilLogger
import com.pg.bigdata.util.validation.Validation
import com.pg.bigdata.util.validation.identifiers.{Number, ThresholdType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CheckNotNullStoreFormat(storeFeatDf: DataFrame) extends Validation {

  override def description: String = "Checks if store_format column does not contain nulls"

  override def thresholdType: ThresholdType = Number

  override def thresholdCriticalOver: Double = 0.0

  override def thresholdGoodTo: Double = 0.0

  override def badRecordsFilter: String = "true"

  override def prepareData(implicit spark: SparkSession, logger: UtilLogger): DataFrame = {
    storeFeatDf.where(store_format.isNull)
  }

}
