name := "TestConsumer"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}