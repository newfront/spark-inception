package com.coffeeco.data

import com.coffeeco.data.config.AppConfig
import com.coffeeco.data.traits.SparkApplication
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkInceptionControllerApp extends SparkApplication {
  val logger: Logger = Logger.getLogger("com.coffeeco.data.SparkInceptionControllerApp")
  //
  override lazy val appConfigProps = AppConfig()
  /**
   * ensure that the application can run correctly, and there is no missing or empty config
   *
   * @param sparkSession The SparkSession
   * @return true if the application is okay to start
   */
  override def validateConfig()(implicit sparkSession: SparkSession) = true

  // Remote Session Controller

}
