package com.coffeeco.data.config

import com.coffeeco.data.traits.SparkAppConfig

case class AppConfig() extends SparkAppConfig {
  override val SinkQueryNameDefault: String = "spark_inception_controller_stream"
  override val SinkToTableNameDefault = "com:coffeeco:notebooks:v1:notebook1:results"
  val UniqueKeyColumn = "paragraphId"
}
