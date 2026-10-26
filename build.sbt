organization := "com.phasmidsoftware"

name := "Majabigwaduce"

version := "2.0.2"

scalaVersion := "3.3.9" // LTS version

val flogVersion = "1.0.15"
val pekkoGroup = "org.apache.pekko"
val pekkoVersion = "1.7.1"
val scalaTestVersion = "3.2.20"
val configVersion = "1.4.9"
val tagSoupVersion = "1.2.1"
val scalaMockVersion = "7.5.5" // 7.6.x requires Scala 3.9
val logBackVersion = "1.5.38"
val scalaXMLVersion = "2.5.0"
Compile / doc / scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
	"com.phasmidsoftware" %% "flog" % flogVersion,
	pekkoGroup %% "pekko-actor-typed" % pekkoVersion,
	pekkoGroup %% "pekko-slf4j" % pekkoVersion,
	pekkoGroup %% "pekko-cluster" % pekkoVersion,
	pekkoGroup %% "pekko-remote" % pekkoVersion,
	pekkoGroup %% "pekko-cluster-metrics" % pekkoVersion,
	"com.typesafe" % "config" % configVersion,
	"ch.qos.logback" % "logback-classic" % logBackVersion % Test,
	pekkoGroup %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
	"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
	"org.scalamock" %% "scalamock" % scalaMockVersion % Test,
// NOTE: xml and tagsoup are for WebCrawler exemplar
  "org.scala-lang.modules" %% "scala-xml" % scalaXMLVersion % Test,
	"org.ccil.cowan.tagsoup" % "tagsoup" % tagSoupVersion % Test
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
		scalaVersion := "3.3.9",
		publish / skip := true,
		libraryDependencies += "ch.qos.logback" % "logback-classic" % logBackVersion % "runtime"
	)
