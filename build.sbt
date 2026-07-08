organization := "com.phasmidsoftware"

name := "Majabigwaduce"

version := "1.0.6"

scalaVersion := "2.13.18"

val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.8.8"
val scalaTestVersion = "3.2.20"
val configVersion = "1.4.9"
val scalaMockVersion = "7.5.5"
val logBackVersion = "1.5.37"
val scalaXMLVersion = "2.4.0"
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits", "-deprecation")

resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
	"com.phasmidsoftware" %% "comparer" % "1.0.9" withSources() withJavadoc(),
	"com.phasmidsoftware" %% "flog" % "1.0.10" withSources() withJavadoc(),
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

Test / unmanagedSourceDirectories += baseDirectory.value / "src/it/scala"
Test / unmanagedResourceDirectories += baseDirectory.value / "src/it/resources"
Test / parallelExecution := false
