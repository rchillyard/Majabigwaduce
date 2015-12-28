name := "Majabigwaduce"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.7"

val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.4.1"
val scalaTestVersion = "2.2.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
	akkaGroup %% "akka-actor" % akkaVersion,
	akkaGroup %% "akka-testkit" % akkaVersion % "test",
	akkaGroup %% "akka-slf4j" % akkaVersion, 
	"com.typesafe" % "config" % "1.3.0",
	"org.scalatest" %% "scalatest" % scalaTestVersion % "test",  
	"ch.qos.logback" % "logback-classic" % "1.0.0" % "runtime",
// xml and tagsoup are for WebCrawler exemplar
    "org.scala-lang.modules" %% "scala-xml" % "1.0.2",
	"org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1"
)
