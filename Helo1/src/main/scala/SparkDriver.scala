package org.example.code
import scala.io.Source
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}
import java.io.{FileNotFoundException, IOException}
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger

object SparkDriver {
  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(getClass.getName)
    log.info("started")
    val spark = SparkSession.builder()
      .master("yarn").appName("SparkSaveCSV").enableHiveSupport().getOrCreate()

    if (args.length == 0) {
      log.info("missing input value config file as input  parameter")
    }

    var configMapper = Map.empty[String, String]
    log.info("inside program1")
    val config_file = args(0)
    val datafile_df = spark.emptyDataFrame

    //Read config files from local
    def ReadConf(config_file:String):Map[String,String] = {
      //val config_file = "hdfs:///conf/conf.properties"
      //val config_file="conf.properties"
      var configMap = Source.fromFile(config_file)
        .getLines().filter(line => line.contains("=")).map { line =>
        val keys = line.split("=")
        if (keys.size == 1) {
          (keys(0) -> "")
        } else {
          (keys(0) -> keys(1))
        }
      }.toMap

      configMap

    }

    log.info("call to configMapper")

    // function call
    configMapper=ReadConf(config_file)

    try {

      def isNullOrEmpty(in_str: String): Boolean = {
        if (in_str == null || in_str.trim.isEmpty)
          true
        else
          false
      }

      //variables
      log.info(configMapper("hive_db"))
      log.info(configMapper("hdfs_path"))
      log.info(configMapper("query"))

      if (
        (!isNullOrEmpty(configMapper("query")))
          &&
          (!isNullOrEmpty(configMapper("hdfs_path")))
      )
      {

        log.info("inside run query" + configMapper("query"))
        spark.sql(" use " + configMapper("hive_db"))
        val datafile_df = spark.sql(configMapper("query"))
        datafile_df.show()
        log.info("dataframe count is 1:" +datafile_df.count())


        if (!datafile_df.head(1).isEmpty) {
          log.info("Saving data into CSV file" + configMapper("hdfs_path"))
          datafile_df.write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", ",").csv(configMapper("hdfs_path"))
        }
        else {
          log.info("source data frame is empty!")
        }
      }

    }

      // Catch clause
    catch {
      case ex: FileNotFoundException => println("FileNotFoundException occurred")
      case ex: IOException => println("IOException occurred")
    }
    finally {
      log.info("In finally block")
      //stopping spark session
      spark.stop()
    }
  }
}
