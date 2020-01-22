name := "Majabigwaduce"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.1"

val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.6.1"
val scalaTestVersion = "3.1.0"
val configVersion = "1.3.1"
val scalaMockVersion = "4.4.0"
val logBackVersion = "1.2.3"
val scalaXMLVersion = "1.2.0"
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
	akkaGroup %% "akka-actor" % akkaVersion,
	akkaGroup %% "akka-testkit" % akkaVersion % "test",
	akkaGroup %% "akka-slf4j" % akkaVersion,
	akkaGroup %% "akka-cluster" % akkaVersion,
	akkaGroup %% "akka-remote" % akkaVersion,
	akkaGroup %% "akka-cluster-metrics" % akkaVersion,
	"com.typesafe" % "config" % configVersion,
	"org.scalatest" %% "scalatest" % scalaTestVersion % "test",
	"org.scalamock" %% "scalamock" % scalaMockVersion % "test",
	"ch.qos.logback" % "logback-classic" % logBackVersion % "runtime",
// xml and tagsoup are for WebCrawler exemplar
  "org.scala-lang.modules" %% "scala-xml" % scalaXMLVersion % "test",
	"org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1" % "test"
	
)

unmanagedSourceDirectories in Test += baseDirectory.value / "src/it/scala"

parallelExecution in Test := false

