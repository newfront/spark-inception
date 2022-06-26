package com.coffeeco.data.processor

import com.coffeeco.data.rpc.NetworkCommand
import org.apache.spark.sql.{Dataset, SparkSession}

object NetworkCommandProcessor {
  def apply(spark: SparkSession): NetworkCommandProcessor = {
    new NetworkCommandProcessor(spark)
  }
}

@SerialVersionUID(1L)
class NetworkCommandProcessor(val spark: SparkSession) extends Serializable {

  def process(ds: Dataset[NetworkCommand]): Dataset[NetworkCommand] = {
    ds
  }

}
