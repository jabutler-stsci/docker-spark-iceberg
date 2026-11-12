import scala.jdk.CollectionConverters._

def createSparkSession() = {
  val defaults = new java.util.Properties()
  val configStream = java.nio.file.Files.newInputStream(
    java.nio.file.Paths.get(sys.env("SPARK_HOME"), "conf", "spark-defaults.conf")
  )
  try defaults.load(configStream) finally configStream.close()
  val config = new org.apache.spark.SparkConf(false)
    .setAll(defaults.asScala.toSeq)

  org.apache.spark.sql.SparkSession.builder()
    .config(config)
    .config("spark.repl.class.outputDir", sys.env("SCALA_OUTPUT_DIR"))
    .appName("Scala notebook")
    .master(sys.env.getOrElse("SPARK_MASTER", "local[2]"))
    .getOrCreate()
}
