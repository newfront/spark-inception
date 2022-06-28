package com.coffeeco.data.traits

import com.coffeeco.data.config._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApplication extends App {

  val appName = Configuration.appName
  def appConfig: AppConfig = AppConfig()

  /**
   * Generate the sparkConf, TypeSafeConfig (tsc: Configuration) is mixed in only if the values
   * are missing after Apache Spark generates its view of the Config (defaults, properties, sys props, etc, then tsc)
   */
  lazy val sparkConf: SparkConf = {
    val coreConf = new SparkConf()
      .setAppName(appName)
    // merge if missing
    Configuration.Spark.settings.foreach(tuple =>
      coreConf.setIfMissing(tuple._1, tuple._2)
    )
    coreConf
  }

  /**
   * Allow for delayed Spark Configuration mixing before generating the sparkSession
   * given sparkConf is also lazy, you will have a lazy invocation chain which is tight
   */
  lazy implicit val sparkSession: SparkSession = {
    SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * ensure that the application can run correctly, and there is no missing or empty config
   * @param sparkSession The implicit SparkSession object
   * @throws RuntimeException with human readable error
   */
  def validateConfig()(implicit sparkSession: SparkSession): Unit = {}

  def run(): Unit = {
    validateConfig()
  }

}
