import sbt.Provided

val CommonsPoolVersion = "2.0"
val JedisVersion = "3.9.0"
val SparkVersion = "3.2.1"

lazy val root = (project in file(".")).settings(
  version := "1.0.0-SNAPSHOT",
  organization := "com.coffeeco.data",
  name := "spark-inception-controller",
  scalaVersion := "2.12.15",
  Compile / mainClass := Some("com.coffeeco.data.SparkInceptionControllerApp"),
  assemblyPackageScala / assembleArtifact := false
)

/*
 note: once you have spark-core,spark-hive,spark-sql, spark-tags available locally
 you can test everything, then switch to provided level scope for assembly, since the docker
 base container already has all the spark jars (save space = $$$ back in your bandwidth pocket)
 */
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % SparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % SparkVersion % Provided,
  "org.apache.spark" %% "spark-tags" % SparkVersion % Provided,
  "org.apache.spark" %% "spark-repl" % SparkVersion % Provided,
  "org.apache.commons" % "commons-pool2" % CommonsPoolVersion % Compile,
  "org.scala-lang" % "scala-library" % scalaVersion.toString() % Provided,
  "org.scala-lang" % "scala-compiler" % scalaVersion.toString() % Provided,
  "org.scala-lang" % "scala-reflect" % scalaVersion.toString() % Provided,
  "redis.clients" % "jedis" % JedisVersion,
  "com.redislabs" %% "spark-redis" % "3.1.0" % Compile,
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.spark" %% "spark-sql" % SparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % SparkVersion % Test classifier "test-sources",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.4.2" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % Test
)

Test / parallelExecution  := false
Test / fork := true
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// See for more details : https://github.com/sbt/sbt-assembly

assembly / assemblyMergeStrategy := {
  case PathList("io","netty", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last  // Added this for 2.1.0 I think
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

// Hadoop contains an old protobuf runtime that is not binary compatible with Protobuf
// with 3.0.0.  We shaded ours to prevent runtime issues.
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll,
  ShadeRule.rename("scala.collection.compat.**" -> "scalacompat.@1").inAll
)