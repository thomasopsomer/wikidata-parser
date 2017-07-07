//
name := "wikidata-parser"
version := "0.0.1"
scalaVersion := "2.10.5"
organization := "asgard"

// sbt spark plugin practice:
// the name of your Spark Package
spName := "asgard/wikidata-parser"
// the Spark Version your package depends on.
sparkVersion := "1.6.3"
// sparkComponents += "mllib" // creates a dependency on spark-mllib.
sparkComponents += "sql"


// dependencies
libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.github.scopt" %% "scopt" % "3.4.0"
)

// docker assembly plugin
enablePlugins(DockerPlugin)

dockerfile in docker := {
  // The assembly task generates a fat JAR file
  val artifact: File = assembly.value
  val artifactTargetPath = s"/home/wikidata-parser/${artifact.name}"
  val log4jConfig: File = new File("log4j.properties")
  new Dockerfile {
    from("asgard/spark")
    add(artifact, artifactTargetPath)
    env("WIKIDATA_PARSER_JAR_PATH", artifactTargetPath)
    add(log4jConfig, "/usr/spark/conf/log4j.properties")
    cmd("/sbin/my_init")
  }
}

imageNames in docker := Seq(
  // Sets the latest tag
  ImageName(s"${organization.value}/${name.value}:latest"),

  // Sets a name with a tag that contains the project version
  ImageName(
    namespace = Some(organization.value),
    repository = name.value,
    tag = Some("v" + version.value)
  )
)