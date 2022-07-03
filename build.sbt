import Dependencies._
import scalikejdbc.{JDBCSettings => _, _}
import scalikejdbc.mapper._
import scala.collection.JavaConverters._

val scala3Version = "3.1.1"

enablePlugins(ScalikejdbcPlugin)

lazy val root = project
  .in(file("."))
  .settings(
    name := "workflow-engine",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.7.0",
      "org.apache.kafka" % "kafka-clients" % "3.1.0",
      "org.postgresql" % "postgresql" % "42.3.3",
      "com.typesafe.play" %% "play-json" % "2.10.0-RC6",
      "org.scalikejdbc" %% "scalikejdbc" % "4.0.0",
      "org.scalikejdbc" %% "scalikejdbc-test"   % "4.0.0"   % "test",
      "org.flywaydb" % "flyway-core" % "8.5.5",
      "org.scalikejdbc" %% "scalikejdbc-config" % "4.0.0",
      scalaTest % Test,
    ),
  )
