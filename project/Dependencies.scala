import sbt._

object Dependencies {

  val akkaVersion = "2.6.18"
  val akkaHttpVersion = "10.2.7"
  val alpakkaVersion = "3.0.4"

  val allDeps = Seq(
    "com.lightbend.akka" %% "akka-stream-alpakka-s3" % alpakkaVersion,
    "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
    "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-xml" % akkaHttpVersion
  )
}
