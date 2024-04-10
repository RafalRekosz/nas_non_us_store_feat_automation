package com.pg.bigdata.nas.non_us.storefeatautomation.utils

sealed trait DataFormat { override val toString: String = "" }

case object Parquet extends DataFormat { override val toString = "parquet"}

case object Csv extends DataFormat { override val toString = "csv"}
