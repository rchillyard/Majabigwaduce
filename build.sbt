organization := "com.phasmidsoftware"

name := "Majabigwaduce"

version := "1.1.1"

scalaVersion := "3.3.8"

val flogVersion = "1.0.15"
val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.8.8"
val scalaTestVersion = "3.2.20"
val configVersion = "1.4.9"
val tagSoupVersion = "1.2.1"
val scalaMockVersion = "7.5.5"
val logBackVersion = "1.5.37"
val scalaXMLVersion = "2.4.0"
Compile / doc / scalacOptions ++= Seq("-deprecation")

resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
	"com.phasmidsoftware" %% "flog" % flogVersion withSources() withJavadoc(),
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
	"org.ccil.cowan.tagsoup" % "tagsoup" % tagSoupVersion % "test"
)

Test / unmanagedSourceDirectories += baseDirectory.value / "src/it/scala"
Test / unmanagedResourceDirectories += baseDirectory.value / "src/it/resources"
Test / parallelExecution := false

lazy val root = project.in(file("."))

lazy val benchmarks = project.in(file("benchmarks"))
	.dependsOn(root)
	.enablePlugins(JmhPlugin)
	.settings(
		name := "majabigwaduce-benchmarks",
		scalaVersion := "3.3.8",
		publish / skip := true,
		libraryDependencies += "ch.qos.logback" % "logback-classic" % logBackVersion % "runtime"
	)
