addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

resolvers ++= Seq(
  "Artima Maven Repository" at "https://repo.artima.com/releases",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "https://dl.bintray.com/typesafe/ivy-releases/",
  "Second Typesafe repo" at "https://dl.bintray.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "https://downloads.mesosphere.io/maven",
  "Maven Repo" at  "https://repo1.maven.org/maven2/",
  "Maven Repo2" at  "https://repo2.maven.org/maven2/",
  Resolver.sonatypeRepo("public")/*,
  Resolver.mavenLocal*/
)
