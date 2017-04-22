
name := "wordCloud"

version := "1.0"

scalaVersion := "2.11.10"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "Hortonworks Jetty Maven Repository" at "http://repo.hortonworks.com/content/repositories/jetty-hadoop/"

libraryDependencies ++= Seq(
  "com.github.scopt" % "scopt_2.11" % "3.5.0",
  "org.apache.kafka" % "kafka_2.11" % "0.10.0.2.5.3.0-37",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0.2.5.3.0-37" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0.2.5.3.0-37" %"provided",
  "org.apache.spark" % "spark-hive_2.11" % "2.0.0.2.5.3.0-37" %"provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.0.2.5.3.0-37" %"provided",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.0.2.5.3.0-37",
  "net.ruippeixotog" %% "scala-scraper" % "1.2.0",
  "com.twitter" %%  "algebird-core"  % "0.11.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("com",   "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com",   "squareup", xs @ _*) => MergeStrategy.last
  case PathList("com",   "sun", xs @ _*) => MergeStrategy.last
  case PathList("com",   "thoughtworks", xs @ _*) => MergeStrategy.last
  case PathList("commons-beanutils", xs @ _*) => MergeStrategy.last
  case PathList("commons-cli", xs @ _*) => MergeStrategy.last
  case PathList("commons-collections", xs @ _*) => MergeStrategy.last
  case PathList("commons-io", xs @ _*) => MergeStrategy.last
  case PathList("io",    "netty", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case PathList("org",   "apache", xs @ _*) => MergeStrategy.last
  case PathList("org",   "codehaus", xs @ _*) => MergeStrategy.last
  case PathList("org",   "fusesource", xs @ _*) => MergeStrategy.last
  case PathList("org",   "mortbay", xs @ _*) => MergeStrategy.last
  case PathList("org",   "tukaani", xs @ _*) => MergeStrategy.last
  case PathList("xerces", xs @ _*) => MergeStrategy.last
  case PathList("xmlenc", xs @ _*) => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.endsWith("sf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf")  => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
