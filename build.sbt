import Dependencies._

val scala3Version = "3.1.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "workflow-engine",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.7.0",
      "org.apache.kafka" % "kafka-clients" % "3.1.0",
      scalaTest % Test,
    )
  )
