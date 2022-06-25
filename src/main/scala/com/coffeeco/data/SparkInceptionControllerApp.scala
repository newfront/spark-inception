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
  override def run(): Unit = {
    logger.info("This block will create the StreamingQuery, and monitor the streaming progress of the app")
    // 1. This application takes advantage of the DataStreamReader and DataStreamWriter interfaces to connect
    // to a Streaming Source / Sink
    // 2. We treat each Row within each MicroBatch as a synchronous series of RPC commands.
    // 2a. Each command is evaluated separately (just like with a traditional
    // notebook environment (notebook - name: tests, paragraphs: Seq(a,b,c,d,e...) etc
    // 2b. Success and Failures are propagated back over the DataStreamWriter (using the streaming sink of your choice)

    // add
    sparkSession.readStream.format("redis")
  }

}
