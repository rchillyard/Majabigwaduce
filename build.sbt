name := "Majabigwaduce"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.12.5"

val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.5.11"
val scalaTestVersion = "3.0.1"

//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
	akkaGroup %% "akka-actor" % akkaVersion,
	akkaGroup %% "akka-testkit" % akkaVersion % "test",
	akkaGroup %% "akka-slf4j" % akkaVersion,
	akkaGroup %% "akka-cluster" % akkaVersion,
	akkaGroup %% "akka-remote" % akkaVersion,
	akkaGroup %% "akka-cluster-metrics" % akkaVersion,
	"com.typesafe" % "config" % "1.3.1",
	"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
	"org.scalamock" %% "scalamock-scalatest-support" % "3.4.2" % "test",
	"ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
// xml and tagsoup are for WebCrawler exemplar
    "org.scala-lang.modules" %% "scala-xml" % "1.1.0",
	"org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1"
	
)

unmanagedSourceDirectories in Test += baseDirectory.value / "src/it/scala"
