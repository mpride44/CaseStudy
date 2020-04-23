

name := "Processing"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies++=Seq("org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.spark" %% "spark-mllib" % "2.3.2",
  "org.apache.kafka" % "kafka-clients" % "2.4.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2")


libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

// https://mvnrepository.com/artifact/org.scalatest/scalatest-funsuite
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.3.0-SNAP2"


libraryDependencies ++= Seq("junit" % "junit" % "4.8.1" % "test")
//------- last added dependencies-------//
// if any error or runtime exceptions remove below  dependecies
//resolvers += Resolver.sonatypeRepo("releases")
//
//libraryDependencies += "com.danielasfregola" %% "twitter4s" % "6.2"

//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"


//
//
//libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"
