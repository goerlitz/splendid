name := """splendid"""

version := "0.1"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.9",
  //
  // testing
  "com.typesafe.akka" %% "akka-testkit" % "2.3.9" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  //
  // 3rd party
  "org.openrdf.sesame" % "sesame-runtime" % "2.7.14",
  "commons-logging" % "commons-logging-api" % "1.1",
  "org.slf4j" % "slf4j-simple" % "1.7.7"
)
