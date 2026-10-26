organization := "com.phasmidsoftware"

name := "Majabigwaduce"

version := "2.0.2"

scalaVersion := "3.3.8"

val flogVersion = "1.0.15"
val pekkoGroup = "org.apache.pekko"
val pekkoVersion = "1.7.1"
val scalaTestVersion = "3.2.20"
val configVersion = "1.4.9"
val tagSoupVersion = "1.2.1"
val scalaMockVersion = "7.5.5"
val logBackVersion = "1.5.37"
val scalaXMLVersion = "2.4.0"
Compile / doc / scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
	"com.phasmidsoftware" %% "flog" % flogVersion withSources() withJavadoc(),
	pekkoGroup %% "pekko-actor-typed" % pekkoVersion withSources() withJavadoc(),
	pekkoGroup %% "pekko-slf4j" % pekkoVersion withSources() withJavadoc(),
	pekkoGroup %% "pekko-cluster" % pekkoVersion withSources() withJavadoc(),
	pekkoGroup %% "pekko-remote" % pekkoVersion withSources() withJavadoc(),
	pekkoGroup %% "pekko-cluster-metrics" % pekkoVersion withSources() withJavadoc(),
	"com.typesafe" % "config" % configVersion withSources() withJavadoc(),
	"ch.qos.logback" % "logback-classic" % logBackVersion % "runtime",
	pekkoGroup %% "pekko-actor-testkit-typed" % pekkoVersion % "test",
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
