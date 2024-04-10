package com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.enterprise.application.nas_engine

import com.pg.bigdata.nas.non_us.storefeatautomation.utils.{Csv, DataFormat, DataSetLoader, DataSetSaver, Parquet, PathProvider, StoreFeatDataSetSaver, application_data, input, nas_engine}
import com.pg.bigdata.nas.non_us.storefeatautomation.datamodel.Columns
import org.apache.spark.sql.Column

trait StoreFeatDataSet extends DataSetLoader with StoreFeatDataSetSaver {

  val name: String = "store_feat.csv"
  override def dataSetFileFormat: DataFormat = Csv
  override def numPartitions: Int = 1
  def path(implicit pathProvider: PathProvider): String =
    s"${pathProvider.basePath}/$application_data/$nas_engine/$marketName/$input/$name"

  val columnsInFile: Seq[Column] = Seq(
    Columns.id,
    Columns.pos_cust_id,
    Columns.pos_store_id,
    Columns.retailer_name,
    Columns.name,
    Columns.store_format,
    Columns.subcluster,
    Columns.city,
    Columns.address,
    Columns.postal_code,
    Columns.lat,
    Columns.long,
    Columns.region,
    Columns.size
  )

}
