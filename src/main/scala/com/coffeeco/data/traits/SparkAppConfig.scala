package com.coffeeco.data.traits

trait SparkAppConfig {
  val ReplClassDir = "spark.repl.classdir"
  val ReplExtraJarsDir = "spark.repl.extra.jars.dir"
  val SourceFormat: String = "spark.app.source.format"
  val SourceFormatDefault: String = "redis"
  val SourceSchemaDDL: String = "spark.app.source.schemaDDL"
  val SourceStreamOptions: String = "spark.app.source.options."

  val SinkFormat: String = "spark.app.sink.format"
  val SinkFormatDefault: String = "parquet"
  val SinkQueryName: String = "spark.app.sink.queryName"
  val SinkQueryNameDefault: String = "output_stream"
  val SinkStreamOptions: String = "spark.app.sink.options."
  val SinkToTableName = "spark.app.sink.output.tableName"
  val SinkToTableNameDefault = "output_table"
}
