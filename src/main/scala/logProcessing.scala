import org.apache.spark.sql._

object LogDataProject {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  /** A class that instantiate an object encapsulating the data at
    * different steps of processing
    *
    * @constructor create a new Log with a path
    * @param path the path to the log data
    */
  class Log(val path: String) extends Serializable {
    val rawData = read(path)
    val cleanData = cleanDate()
    val reportData = highConnections()

    /** A method to read and load raw data into a spark Dataset
      *
      * @param path the path to the log data
      * @return a spark Dataset[String] containing raw data
      */
    private def read(path: String): Dataset[String] = {
      spark.read.textFile(path)
    }

    /** Method to extract {date} - {Ip_address} - {URI} data. The use
      * of the functions.regexp_extract returns the matching group if found
      * or an empty string. Rows containing empty string in date and Ip have
      * been drop (their count is 1 for each). We did keep the row with empty
      * string for URI (around 50000)
      *
      * @param logData a Dataset[String] holding log raw data
      * @return a spark Dataset
      */
    private def extractDateIpUri(logData: Dataset[String]): DataFrame = {
      val patternIp = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})"
      val patternDate = "(\\d{2}/\\w{3}/\\d{4})"
      val patternUri = """"[A-Z]+ (\/.+) HTTP\/\d\.\d""""
      logData
        .withColumn(
          "rawDate",
          functions.regexp_extract(functions.col("value"), patternDate, 1)
        )
        .withColumn(
          "Ip_address",
          functions.regexp_extract(functions.col("value"), patternIp, 1)
        )
        .withColumn(
          "Uri",
          functions.regexp_extract(functions.col("value"), patternUri, 1)
        )
        .drop(
          "value"
        )
        .filter(
          functions.col("Ip_address") =!= ""
        )
        .filter(
          functions.col("rawDate") =!= ""
        )
    }

    /** Method to cast the Date string into proper Date type
      *
      * @return a spark DataFrame
      */
    private def cleanDate(): DataFrame = {
      val patternDate = "dd/MMM/yyyy"
      val dataWithCleanDate = extractDateIpUri(rawData)
        .withColumn(
          "Date",
          functions.to_date(functions.col("rawDate"), patternDate)
        )

      dataWithCleanDate
        .drop("rawDate")
    }

    /** Method to get the count of Ip_address by date
      *
      * @param logData
      * @return a spark DataFrame
      */
    private def getIpCountByDate(
        logData: DataFrame = this.cleanData
    ): DataFrame = {
      logData
        .groupBy("Date", "Ip_address")
        .count()
        .withColumnRenamed("count", "Ip_Count")
        .withColumnRenamed("Date", "Date_Ip")
    }

    /** Method to get the count of URI by date
      *
      * @param logData
      * @return a spark DataFrame
      */
    private def getUriCountByDate(
        logData: DataFrame = this.cleanData
    ): DataFrame = {
      logData
        .groupBy("Date", "Uri")
        .count()
        .withColumnRenamed("count", "Uri_Count")
        .withColumnRenamed("Date", "Date_Uri")
    }

    /** Method to join two spark Dataframes: one with the count of ip addresses
      * by date and the other with the count of URI by date
      *
      * @param ip a Spark DataFrame obtained from getIpCountByDate
      * @param uri a spark DataFrame obtained from getUriCountByDate
      * @param joinType type of join to perform on the two DataFrame
      * @return joined spark DataFrame
      */
    private def joinIpAndUri(
        ip: DataFrame,
        uri: DataFrame,
        joinType: String
    ): DataFrame = {
      ip.join(uri, ip("Date_Ip") === uri("Date_Uri"), joinType)
    }

    /** Method that return a spark DataFrame with URI count above a certain
      * threshold. By default threshold = 20000
      *
      * @param threshold is a number
      * @return a spark DataFrame
      */
    private def highConnections(threshold: Int = 20000): DataFrame = {
      joinIpAndUri(
        getIpCountByDate(cleanData),
        getUriCountByDate(cleanData),
        "left"
      ).select("*")
        .where(functions.col("Uri_count") > threshold)
        .drop("Date_Uri")
    }
  }

  /** Companion of the Log class. The apply method is used as a Factory method
    * to instantiate the Log class
    */
  object Log {
    def apply(path: String): Log = new Log(path)
  }
}
