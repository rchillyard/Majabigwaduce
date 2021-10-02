organization := "com.phasmidsoftware"

name := "Majabigwaduce"

version := "1.0.5"

scalaVersion := "2.13.6"

val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.6.16"
val scalaTestVersion = "3.2.9"
val configVersion = "1.4.1"
val scalaMockVersion = "5.1.0"
val logBackVersion = "1.2.6"
val scalaXMLVersion = "2.0.1"
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits", "-deprecation")

resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"
//resolvers += "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/comphasmidsoftware-1001"

libraryDependencies ++= Seq(
	"com.phasmidsoftware" %% "comparer" % "1.0.9" withSources() withJavadoc(),
	"com.phasmidsoftware" %% "flog" % "1.0.8" withSources() withJavadoc(),
	akkaGroup %% "akka-actor" % akkaVersion withSources() withJavadoc(),
	akkaGroup %% "akka-slf4j" % akkaVersion withSources() withJavadoc(),
	akkaGroup %% "akka-cluster" % akkaVersion withSources() withJavadoc(),
	akkaGroup %% "akka-remote" % akkaVersion withSources() withJavadoc(),
	akkaGroup %% "akka-cluster-metrics" % akkaVersion withSources() withJavadoc(),
	"com.typesafe" % "config" % configVersion withSources() withJavadoc(),
	"ch.qos.logback" % "logback-classic" % logBackVersion % "runtime",
	akkaGroup %% "akka-testkit" % akkaVersion % "test",
	"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
	"org.scalamock" %% "scalamock" % scalaMockVersion % "test",
// NOTE: xml and tagsoup are for WebCrawler exemplar
  "org.scala-lang.modules" %% "scala-xml" % scalaXMLVersion % "test",
	"org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1" % "test"
)

unmanagedSourceDirectories in Test += baseDirectory.value / "src/it/scala"
unmanagedResourceDirectories in Test += baseDirectory.value / "src/it/resources"

parallelExecution in Test := false
