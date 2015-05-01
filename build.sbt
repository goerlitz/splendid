name := """splendid"""

version := "0.1"

scalaVersion := "2.11.6"

// Add scalac option to show feature warnings
scalacOptions ++= Seq("-feature")

// Include only src/[main|test]/scala in the compile/test configuration
unmanagedSourceDirectories in Compile := (scalaSource in Compile).value :: Nil
unmanagedSourceDirectories in Test    := (scalaSource in Test).value :: Nil


resolvers += "Restlet repository required by LinkedDataServer" at "http://maven.restlet.com"

libraryDependencies ++= Seq(

  // scala libraries
  "com.typesafe.akka" %% "akka-actor"   % "2.3.10",
  
  // testing
  "com.typesafe.akka" %% "akka-testkit" % "2.3.10" % "test",
  "org.scalacheck"    %% "scalacheck"   % "1.12.2" % "test",
  "org.scalatest"     %% "scalatest"    % "2.2.4"  % "test",
  
  // 3rd party
  "org.openrdf.sesame"       % "sesame-runtime"     % "2.8.1",
  "net.fortytwo.sesametools" % "linked-data-server" % "1.9",
  
  "commons-logging" % "commons-logging-api" % "1.1",
  "org.slf4j"       % "slf4j-simple"        % "1.7.12"
)
