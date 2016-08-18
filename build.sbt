name := """splendid"""

version := "0.1"

scalaVersion := "2.11.8"

// Add scalac options for ensuring safer Scala code
scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",         // warn if deprecated APIs are used
  "-feature",             // warn if features are used that should be imported explicitly
  "-unchecked",           // warn if generated code depends on assumptions

  "-Xlint",               // Enable recommended additional warnings

  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-unused-import"
)

// Fail the compilation if there are any warnings (excluding tests)
scalacOptions in (Compile, compile) += "-Xfatal-warnings"

// Include only src/[main|test]/scala in the compile/test configuration
unmanagedSourceDirectories in Compile := (scalaSource in Compile).value :: Nil
unmanagedSourceDirectories in Test    := (scalaSource in Test).value :: Nil


resolvers += "Restlet repository required by LinkedDataServer" at "http://maven.restlet.com"

libraryDependencies ++= Seq(

  // compile
  "com.typesafe.akka" %% "akka-actor"   % "2.4.8",
  
  // testing
  "com.typesafe.akka" %% "akka-testkit" % "2.4.8"  % "test",
  "org.scalacheck"    %% "scalacheck"   % "1.13.2" % "test",
  "org.scalatest"     %% "scalatest"    % "3.0.0"  % "test",
  
  // 3rd party
  "org.openrdf.sesame"       % "sesame-runtime"     % "4.1.2",
  "net.fortytwo.sesametools" % "linked-data-server" % "1.10",

  // logging
  "org.slf4j"       % "slf4j-simple"        % "1.7.21"
)
