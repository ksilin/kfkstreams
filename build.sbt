import sbt.Keys.version

lazy val kfkstreams = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(settings)
  .settings(
    libraryDependencies ++= Seq(
      library.kafkaStreamsScala,
      library.kafka,
      library.curator,
      library.catsCore,
      library.logback,
      library.scalaLogging,
      library.kafkabase,
      library.circe,
      library.circeGeneric,
      library.circeParser,
      library.circeJ8,
      library.circeJackson,
      library.scalaCheck % Test,
      library.scalaTest  % Test
    ),
    excludeDependencies += "org.slf4j" % "slf4j-log4j12"
  )

lazy val library = new {
  object Version {
    val scalaCheck   = "1.13.5"
    val scalaTest    = "3.0.4"
    val logback      = "1.2.3"
    val scalaLogging = "3.7.2"

    val kafkabase = "0.1.6"
    val circe     = "0.9.1"

    val kafkaStreamsScala = "0.1.1"
    val kafka             = "1.0.0"
    val curator           = "4.0.0"
  }

  val kafkaStreamsScala = "com.lightbend" %%
  "kafka-streams-scala" % Version.kafkaStreamsScala

  val kafka   = "org.apache.kafka"   %% "kafka"       % Version.kafka
  val curator = "org.apache.curator" % "curator-test" % Version.curator

  val logback      = "ch.qos.logback"             % "logback-classic" % Version.logback
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging"  % Version.scalaLogging
  val catsCore     = "org.typelevel"              %% "cats-core"      % "1.0.0"

  val circe        = "io.circe" %% "circe-core"    % Version.circe
  val circeGeneric = "io.circe" %% "circe-generic" % Version.circe
  val circeParser  = "io.circe" %% "circe-parser"  % Version.circe
  val circeJ8      = "io.circe" %% "circe-java8"   % Version.circe
  val circeJackson = "io.circe" %% "circe-jackson" % Version.circe

  val kafkabase = "de.exellio" %% "kafkabase" % Version.kafkabase

  val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck
  val scalaTest  = "org.scalatest"  %% "scalatest"  % Version.scalaTest
}

lazy val settings =
  commonSettings
//++ scalafmtSettings

lazy val commonSettings =
  Seq(
    // scalaVersion from .travis.yml via sbt-travisci
    // scalaVersion := "2.12.4",
    organization := "de.example",
    organizationName := "example",
    name := "kfkstreams",
    version := "0.0.1",
    startYear := Some(2018),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding",
      "UTF-8",
      //"-Xexperimental",
      "-feature", // warn about misused language features
      "-Xlint",   // enable handy linter warnings
//      "-Xfatal-warnings", // turn compiler warnings into errors
      "-Ypartial-unification" // allow the compiler to unify type constructors of different arities
    ),
    unmanagedSourceDirectories.in(Compile) := Seq(scalaSource.in(Compile).value),
    unmanagedSourceDirectories.in(Test) := Seq(scalaSource.in(Test).value)
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true
  )

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
