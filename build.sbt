name := "akka-distributed-workers-java"
version := "0.1"
scalaVersion := "2.11.7"
lazy val akkaVersion = "2.4.0-RC1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "org.iq80.leveldb" % "leveldb" % "0.7",
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test->default",
  "commons-io" % "commons-io" % "2.4" % "test")

fork in Test := true
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
compileOrder := CompileOrder.JavaThenScala
