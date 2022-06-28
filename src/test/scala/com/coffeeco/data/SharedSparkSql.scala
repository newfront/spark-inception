package com.coffeeco.data

import com.holdenkarau.spark.testing.{LocalSparkContext, SparkContextProvider}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSql extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  @transient var _sparkSql: SparkSession = _
  @transient private var _sc: SparkContext = _

  override val sc: SparkContext = _sc

  def conf: SparkConf
  val sparkSql: SparkSession = _sparkSql

  override def beforeAll() {
    _sparkSql = SparkSession.builder().config(conf).getOrCreate()

    _sc = _sparkSql.sparkContext
    setup(_sc)
    super.beforeAll()
  }

  override def afterAll() {
    try {
      // given we are using a Singleton SparkSession
      // based on the SparkInceptionControllerApp::sparkSession lazy val
      // we can't close the SQLContext or SparkContext
      // otherwise the tests will fail
      /*_sparkSql.close()
      _sparkSql = null
      LocalSparkContext.stop(_sc)
      _sc = null*/
    } finally {
      super.afterAll()
    }
  }
}
