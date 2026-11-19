// Optional but useful
ThisBuild / organizationName := "Phasmid Software"
ThisBuild / organizationHomepage := Some(url("https://phasmidsoftware.com/"))
ThisBuild / description := "Scala library for parallel computation"
ThisBuild / homepage := Some(url("https://github.com/rchillyard/Majabigwaduce"))

// Publishing configuration
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
ThisBuild / publishMavenStyle := true

// Credentials
ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

// Required POM metadata for Maven Central
ThisBuild / licenses := List("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/rchillyard/Majabigwaduce"),
    "scm:git@github.com:rchillyard/Majabigwaduce.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "rchillyard",
    name  = "Robin Hillyard",
    email = "rchillyard@phasmidsoftware.com",
    url   = url("https://phasmidsoftware.com")
  )
)

// Optional

// Publishing settings
// PVP: the first two numbers form the major version, so a patch release must stay binary
// compatible while a minor release may break, on purpose and with a release note. Under the
// previous "early-semver" setting, sbt and Coursier treated x.(y+1) as compatible with x.y and
// would evict silently, which is how a removed method becomes a downstream NoSuchMethodError.
ThisBuild / versionScheme := Some("pvp")
ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

// sbt's lint wrongly reports these ThisBuild publish settings as unused
Global / excludeLintKeys ++= Set(pomIncludeRepository, publishMavenStyle)
