package com.coffeeco.data

import com.coffeeco.data.TestHelper.fullPath
import com.coffeeco.data.config.AppConfig
import com.coffeeco.data.controller.SparkRemoteSession
import com.coffeeco.data.processor.NetworkCommandProcessor
import com.coffeeco.data.rpc.NetworkCommand
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkInceptionControllerAppSpec extends AnyFlatSpec with Matchers with SharedSparkSql {
  val appConfig: AppConfig = AppConfig()

  override def conf: SparkConf = {
    val sparkWarehouseDir = fullPath("src/test/resources/spark-warehouse")
    val testConfigPath = fullPath("src/test/resources/application-test.conf")
    sys.props += (("config.file", testConfigPath))
    sys.props += (("HADOOP_USER_NAME", "anyone"))

    /*
    "spark.redis.host" = "redis"
     "spark.redis.port" = "6379"
      "spark.app.source.format" = "redis" # source format
      "spark.app.source.options.stream.keys" = "com:coffeeco:notebooks:v1:notebook1:rpc" # the source redis stream
      "spark.app.source.options.stream.read.batch.size" = "10" #take up to 100 records per batch
      "spark.app.source.options.stream.read.block" = "1000" #wait up to 1 second while fetching new data
     */

    /*
    source.stream = com:coffeeco:notebooks:v1:notebook-tests:rpc
    sink.table = com:coffeeco:notebooks:v1:notenook-tests:results
     */
    SparkInceptionControllerApp.sparkConf
      .setMaster("local[*]")
      .set("spark.app.id", appID)
      .set("spark.driver.cores", "2")
      .set("spark.sql.warehouse.dir", sparkWarehouseDir)
      .set("spark.redis.host", "redis")
      .set("spark.redis.port", "6379")
      .set("spark.app.source.format", "redis")
      .set("spark.app.source.options.stream.keys", "com:coffeeco:notebooks:v1:notebook-tests:rpc")
      .set("spark.app.source.options.stream.read.batch.size", "10")
      .set("spark.app.source.options.stream.read.block", "1000")
      .set(appConfig.SinkToTableName, "com:coffeeco:notebooks:v1:notenook-tests:results")
  }

  lazy val remoteSession: SparkRemoteSession[_] = SparkRemoteSession()

  // note: testing that things work here can be done using locally running redis
  // I know this is a bad practice, but just use the notes from  `spark-inception/local/README.md`
  // to start the local redis docker instance

  "SparkInceptionControllerApp" should " process xstream commands from redis" in {
    implicit val testSession: SparkSession = SparkInceptionControllerApp
      .sparkSession
      .newSession()
    import testSession.implicits._
    implicit val sqlContext: SQLContext = testSession.sqlContext

    val networkCommands = Seq(
      NetworkCommand(
        "notebook1",
        "paragraph1",
        command = "spark.sparkContext.sparkUser",
        "request1", Some("anyone")),
      NetworkCommand(
        "notebook1",
        "paragraph1",
        command = "spark.sql(\"show tables\").show(10)",
        "request2", None),
      NetworkCommand(
        "notebook1",
        "paragraph2",
        command =
          """
            |case class Person(name: String, age: Int)
            |val people = Seq(Person("scott",37),Person("willow",12),Person("clover",6))
            |val df = spark.createDataFrame(people)
            |df.createOrReplaceTempView("people")
            |""".stripMargin,
        "request3", Some("anyone")),
      NetworkCommand("notebook1", "paragraph3",
        command =
          """
            |spark.sql("show tables").show(10)
            |""".stripMargin,
        "request4", Some("anyone")),
      NetworkCommand("notebook1", "paragraph5",
        command =
          """
            |%sql
            |spark.sql("select * from people").limit(10).show(10)
            |""".trim.stripMargin,
        "request6", Some("anyone"))
    )

    val commandStream = MemoryStream[NetworkCommand]
    // add the whole notebook from SparkRemoteSessionSpec
    commandStream.addData(networkCommands)

    val processor = NetworkCommandProcessor(SparkInceptionControllerApp.sparkSession)

    val commandPipeline = SparkInceptionControllerApp
      .outputStream(processor.process(commandStream.toDS()).writeStream)
      .foreachBatch( (ds: Dataset[NetworkCommand], batchId: Long) => {
        SparkInceptionControllerApp.processBatch(ds, batchId)
      })

    val streamingQuery = commandPipeline.start()
    streamingQuery.processAllAvailable()
    streamingQuery.stop()


  }

}
