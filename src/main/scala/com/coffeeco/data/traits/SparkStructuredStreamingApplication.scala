package com.coffeeco.data.traits

import com.coffeeco.data.config.AppConfig
import com.coffeeco.data.listeners.QueryListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}

/**
 * Creates a generic mix-in for Spark Structured Streaming Applications
 *
 * @tparam T The type of either DataFrame or Dataset[_]
 * @tparam U The type parameter for the DataStreamWriter
 */
trait SparkStructuredStreamingApplication[T, U] extends SparkApplication {

  /**
   * The appConfig values will be tested in the base run method
   * the expectations are that bad configuration throws RuntimeException
   *
   * @param sparkSession The implicit SparkSession
   * @throws RuntimeException if the sparkConf is missing critical values
   */
  override def validateConfig()(implicit sparkSession: SparkSession): Unit = {
    // note: this is a basic example. Simply, need to ensure we have at least source/sink options
    if (
      sparkConf.get(appConfig.SourceFormat, "").nonEmpty &&
        sparkConf.get(appConfig.SinkFormat, "").nonEmpty &&
        sparkConf.getAllWithPrefix(appConfig.SourceStreamOptions).nonEmpty &&
        sparkConf.getAllWithPrefix(appConfig.SinkStreamOptions).nonEmpty) {
      // nothing to do here - clap maybe!
    } else throw new RuntimeException(s"You are missing either your source or stream config options. " +
      s"Check that ${appConfig.SourceFormat}, ${appConfig.SinkFormat}, ${appConfig.SourceStreamOptions}, and " +
      s"${appConfig.SinkStreamOptions} are configured")
  }

  /**
   * Takes the implicit SparkSession (spark) context from the mixed-in context
   * then returns a DataStreamReader for the StructuredStreaming application
   * @param spark The implicit SparkSession object
   * @return The inbound (source) DataStreamReader
   */
  def streamReader()(implicit spark: SparkSession): DataStreamReader = {
    val conf = spark.sparkContext.getConf
    val options = conf.getAllWithPrefix(appConfig.SourceStreamOptions).toMap[String, String]
    spark.readStream
      .format(conf.get(appConfig.SourceFormat, appConfig.SourceFormatDefault))
      .options(options)
  }

  /**
   * use the inputStream for lazy invocation of the DataStreamReader
   */
  lazy val inputStream: DataStreamReader = streamReader

  def outputStream(writer: DataStreamWriter[U])(implicit sparkSession: SparkSession): DataStreamWriter[U] = {
    writer
  }

  /**
   * Your application will take a DataStreamReader, do something with the inbound micro-batch data
   * and then ultimately the data will flow back out of the application, through a DataStreamWriter
   *
   * @return The StreamingQuery (which is the full source->transform->sink.start)
   */
  def runApp(): StreamingQuery

  /**
   * Ensure the application configuration is correct, wire up the application, add the streaming query listener
   * and then await either the main query termination or any query termination (if multiple streaming queries)
   */
  override def run(): Unit = {
    super.run()
    runApp()
    sparkSession.streams.addListener(QueryListener())
    awaitAnyTermination()
  }

  def awaitTermination(query: StreamingQuery): Unit = {
    query.awaitTermination()
  }

  def awaitAnyTermination(): Unit = {
    sparkSession.streams.awaitAnyTermination()
  }

}
