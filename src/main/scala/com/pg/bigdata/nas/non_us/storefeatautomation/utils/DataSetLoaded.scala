package com.pg.bigdata.nas.datamodel.utils

import com.pg.bigdata.util.logger.UtilLogger
import org.apache.spark.sql.DataFrame

sealed trait DataSetFrame {
  def dataSet: DataSetProcessor
  def df: DataFrame
}

sealed trait DataSetTransformer[D <: DataSetProcessor] extends DataSetFrame {
  def transform(implicit logger: UtilLogger): DataSetTransformed[D] = {
    val dfTransformed: DataFrame = dataSet.transform(df)
    DataSetTransformed(dataSet, dfTransformed)
  }
}

sealed case class DataSetLoaded[D <: DataSetProcessor](dataSet: DataSetProcessor, df: DataFrame)
  extends DataSetTransformer[D] with DataSetFrame{
  def validateRaw(implicit logger: UtilLogger): DataSetValidatedRaw[D] = {
    dataSet.validateRaw(df)
    DataSetValidatedRaw(dataSet, df)
  }
  def cache(implicit logger: UtilLogger): DataSetLoaded[D] =
    DataSetLoaded(dataSet, df.cache)

  def localCheckpoint(implicit logger: UtilLogger): DataSetLoaded[D] =
    DataSetLoaded(dataSet, df.localCheckpoint)
}

sealed case class DataSetValidatedRaw[D <: DataSetProcessor](dataSet: DataSetProcessor, df: DataFrame)
  extends DataSetTransformer[D] with DataSetFrame

sealed case class DataSetTransformed[D <: DataSetProcessor](dataSet: DataSetProcessor, df: DataFrame)
  extends DataSetFrame {
  def validate(implicit logger: UtilLogger): DataSetValidated[D] = {
    dataSet.validate(df)
    DataSetValidated(dataSet, df)
  }
  def cache(implicit logger: UtilLogger): DataSetTransformed[D] =
    DataSetTransformed(dataSet, df.cache)

  def localCheckpoint(implicit logger: UtilLogger): DataSetTransformed[D] =
    DataSetTransformed(dataSet, df.localCheckpoint)
}

sealed case class DataSetValidated[D <: DataSetProcessor](dataSet: DataSetProcessor, df: DataFrame)
  extends DataSetFrame