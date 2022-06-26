import sbt.Provided

val CommonsPoolVersion = "2.0"
val JedisVersion = "3.9.0"
val SparkVersion = "3.2.1"

lazy val root = (project in file(".")).settings(
  version := "1.0.0-SNAPSHOT",
  organization := "com.coffeeco.data",
  name := "spark-inception-controller",
  scalaVersion := "2.12.12",
  Compile / mainClass := Some("com.coffeeco.data.SparkInceptionControllerApp")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion /*% "provided"*/,
  "org.apache.spark" %% "spark-hive" % SparkVersion /*% "provided"*/,
  "org.apache.spark" %% "spark-sql" % SparkVersion /*% "provided"*/,
  "org.apache.spark" %% "spark-tags" % SparkVersion,
  "org.apache.spark" %% "spark-repl" % SparkVersion,
  "org.apache.commons" % "commons-pool2" % CommonsPoolVersion,
  "org.scala-lang" % "scala-library" % scalaVersion.toString() % Compile,
  "org.scala-lang" % "scala-compiler" % scalaVersion.toString() % Compile,
  "org.scala-lang" % "scala-reflect" % scalaVersion.toString() % Compile,
  "redis.clients" % "jedis" % JedisVersion,
  "com.redislabs" %% "spark-redis" % "3.1.0" % Compile,
  "com.typesafe" % "config" % "1.3.1",
  "org.scalactic" %% "scalactic" % "3.2.0",
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
  case "module-info.class" => MergeStrategy.discard
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case _ =>
    MergeStrategy.discard
}

// Hadoop contains an old protobuf runtime that is not binary compatible with Protobuf
// with 3.0.0.  We shaded ours to prevent runtime issues.
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll,
  ShadeRule.rename("scala.collection.compat.**" -> "scalacompat.@1").inAll
)