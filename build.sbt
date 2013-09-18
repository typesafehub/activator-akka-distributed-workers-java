name := "akka-distributed-workers-java"

version := "0.1"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-contrib" % "2.2.1",
  "com.typesafe.akka" %% "akka-testkit" % "2.2.1",
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test->default")

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
