import Dependencies._
import scalikejdbc.{JDBCSettings => _, _}

val scala3Version = "3.1.3"

enablePlugins(ScalikejdbcPlugin)

lazy val root = project
  .in(file("."))
  .settings(
    name := "workflow-engine",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,
    scalacOptions ++= Seq(
      "-explain-types"
    ),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.7.0",
      "org.apache.kafka" % "kafka-clients" % "3.2.0",
      "org.postgresql" % "postgresql" % "42.3.3",
      "com.typesafe.play" %% "play-json" % "2.10.0-RC6",
      "org.scalikejdbc" %% "scalikejdbc" % "4.0.0",
      "org.scalikejdbc" %% "scalikejdbc-test"   % "4.0.0"   % "test",
      "org.scalikejdbc" %% "scalikejdbc-config" % "4.0.0",
      "org.flywaydb" % "flyway-core" % "8.5.5",
      "org.scalatestplus" %% "mockito-4-5" % "3.2.12.0" % "test",
      scalaTest % Test,
    ),
  )
