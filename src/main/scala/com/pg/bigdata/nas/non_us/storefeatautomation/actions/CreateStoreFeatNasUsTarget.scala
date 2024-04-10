package com.pg.bigdata.nas.non_us.storefeatautomation.actions

import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.Columns
import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.Columns.{cust_id, data_provider_code, data_provider_name, jp_site_id, period_group_end_date_part, pos_sales_amt, site_key, source_data_provider_code}
import com.pg.bigdata.nas.non_us.storefeatautomation.utils.{DataSetSaver, colToString, seqOfColsToSeqOfString}
import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.enterprise.pos_refined.{DataProvider, PosStoreWeekFct, SiteDim, SiteDimExtJp}
import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.application.nas_engine.StoreFeatJapan
import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.application.nas_engine.StoreFeatJapan.{nielsen_area_id, site_state_name}
import com.pg.bigdata.nas.non_us.storefeatautomation.utils.DataSetSaver.DataSetSaverFunctions
import com.pg.bigdata.nas.non_us.storefeatautomation.{ActionTemplate, Env}
import com.pg.bigdata.util.logger.UtilLogger
import org.apache.spark.sql.functions.{col, current_date, date_add, max, min, regexp_replace, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


class CreateStoreFeatJapan(env: Env) extends ActionTemplate(env) {

  implicit val utilLogger: UtilLogger = logger
  implicit val sparkSession: SparkSession = spark

  def run(): Unit = {
    val siteDimExtJpDf: DataFrame = SiteDimExtJp.load.df.where(!cust_id.isin("3869", "3799")).cache()
    val jpSsidsDf: DataFrame = siteDimExtJpDf.select(source_data_provider_code).distinct().cache()

    val siteDimDf: DataFrame = SiteDim.load.df.cache()
    val dataProviderDf: DataFrame = DataProvider.load.df.cache()
    val siteDimJpDf: DataFrame = siteDimDf.alias("site").join(siteDimExtJpDf.alias("site_ext"), Seq(cust_id, site_key)).select("site.*").cache()
    val posStoreWeekFctDf: DataFrame = PosStoreWeekFct.load.df
    val sitesWithSales: DataFrame = posStoreWeekFctDf.join(siteDimExtJpDf, Seq(cust_id, site_key))
      .where(period_group_end_date_part >= date_add(current_date(), -365))
      .groupBy(cust_id, site_key, jp_site_id)
      .agg(sum(pos_sales_amt).alias(pos_sales_amt))
      .where(pos_sales_amt > 1)
      .cache()

    val dataProviderNameDf: DataFrame = dataProviderDf.alias("dp").join(jpSsidsDf.alias("ext"), data_provider_code === source_data_provider_code)
      .select(data_provider_code, data_provider_name)
      .cache()

    val id: Column = site_key.alias(Columns.id)
    val posCustId: Column = regexp_replace(data_provider_code, "cds_", "").alias(Columns.pos_cust_id)
    val retailerName: Column = data_provider_name.alias(Columns.retailer_name)
    val posStoreId: Column = col("jp_site_id").alias(Columns.pos_store_id)
    val name: Column = col("jp_site_alter_lang_name").alias(Columns.name)
    val storeFormat: Column = regexp_replace(col("channel_name"), "NULL", "-").alias(Columns.store_format)
    val storeFormatNotNull: Column = when(storeFormat.isNull, "-").otherwise(storeFormat).alias(Columns.store_format)
    val subcluster: Column = regexp_replace(col("channel_name"), "NULL", "-").alias(Columns.subcluster)
    val subclusterNotNull: Column = when(subcluster.isNull, "-").otherwise(subcluster).alias(Columns.subcluster)
    val city: Column = col("jp_site_city_name").alias(Columns.town_name)
    val address: Column = col("jp_site_street_address").alias(Columns.street)
    val postalCode: Column = col("jp_site_post_code").alias(Columns.postal_code)
    val lat: Column = col("jp_site_latitude").alias(Columns.lat)
    val long: Column = col("jp_site_longitude").alias(Columns.long)
    val region: Column = col("jp_site_region_name").alias(Columns.region)
    val size: Column = (round(pos_sales_amt / 1000) + 1).alias(Columns.size)

    val cols: Seq[Column] = Seq(id, posStoreId, posCustId, retailerName, name, storeFormatNotNull, subclusterNotNull, region, city, address, postalCode, lat, long, size, site_state_name, nielsen_area_id)

    val jpStoreFeatDf: DataFrame = siteDimExtJpDf
      .join(siteDimJpDf, Seq(cust_id, site_key))
      .join(sitesWithSales, Seq(cust_id, site_key))
      .join(dataProviderNameDf, Seq(data_provider_code, data_provider_name))
      .select(cols: _*)
      .where((Columns.lat.isNotNull and Columns.long.isNotNull) or (Columns.street.isNotNull and Columns.town_name.isNotNull and Columns.postal_code.isNotNull))
      .cache()

    val jpStoreFeatFinalDf: DataFrame = jpStoreFeatDf
      .groupBy(Columns.pos_store_id)
      .agg(min(Columns.id).alias(Columns.id),
        min(Columns.pos_cust_id).alias(Columns.pos_cust_id),
        min(Columns.retailer_name).alias(Columns.retailer_name),
        min(Columns.name).alias(Columns.name),
        max(Columns.store_format).alias(Columns.store_format),
        max(Columns.subcluster).alias(Columns.subcluster),
        min(Columns.region).alias(Columns.region),
        min(Columns.town_name).alias(Columns.town_name),
        min(Columns.street).alias(Columns.street),
        min(Columns.postal_code).alias(Columns.postal_code),
        min(Columns.lat).alias(Columns.lat),
        min(Columns.long).alias(Columns.long),
        sum(Columns.size).alias(Columns.size),
        min(site_state_name).alias(site_state_name),
        min(nielsen_area_id).alias(nielsen_area_id))

    StoreFeatJapan.validateStoreFeat(jpStoreFeatFinalDf, env)
    jpStoreFeatFinalDf.saveToAsSingleFile(StoreFeatJapan)
  }
}