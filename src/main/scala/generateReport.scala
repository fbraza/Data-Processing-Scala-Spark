import java.io._
import LogDataProject._
import org.apache.spark.sql.functions
import scala.collection.mutable._
import io.circe.generic.auto._, io.circe.syntax._

object report {

  /** Case class use as an instance matching for our json report
    *
    * @param date a string date
    * @param ipCount an Json object from circe library. Encapsulate an iternal
    * json representation of a scala Map[String, Long] data structure (key: ip
    * address, value: count)
    * @param uriCount an Json object from circe library. Encapsulate an iternal
    * json representation of a scala Map[String, Long] data structure (key:
    * uri, value: count)
    */
  case class jsonReport(
      val date: String,
      val ipCount: io.circe.Json,
      val uriCount: io.circe.Json
  )

  /** Function defined to create a report from the logs data. the function
    * find all the dates having too big number of connection (> 20000) and for
    * each date:
    * - compute the total number of connexion to each URI
    * - compute all the address that access to these URI and the number of time
    *   they accessit on that day
    * @param gzPath is the path poiting to access.log.gz
    * @param outputPath is the path pointing to the output file that is a report
    * in json format with one json report line per date
    */
  def createReport(
      gzPath: String,
      outputPath: String = "logReport.txt"
  ): Unit = {
    val file = new PrintWriter(new File(outputPath))
    val log = Log(gzPath)
    val data = log.reportData
    data.cache()
    // get unique dates
    val unique_dates = data
      .select("Date_Ip")
      .distinct()
      .rdd
      .map(x => x.get(0).toString())
      .collect()
    for (date <- unique_dates) {
      // get unique uri for each day
      val unique_uri = data
        .select("Uri")
        .where(functions.col("Date_Ip") === date)
        .distinct()
        .rdd
        .map(x => x.getString(0))
        .collect
      for (uri <- unique_uri) {
        // initialize an empty Map
        var ipCountDict = Map[String, Long]()
        // generate a temporary dataframe
        val dfTemp = data
          .select("Ip_address", "Ip_Count", "Uri", "Uri_Count")
          .where(
            functions.col("Date_Ip") === date && functions.col("Uri") === uri
          )
        // get the current Uri_count
        val uri_count = dfTemp
          .select("Uri_Count")
          .take(1)
          .head
          .getLong(0)
        // create uri count Map
        val uriCountDict = Map[String, Long](uri -> uri_count)
        // collect data from data frame and feed the Map for ip_counts
        dfTemp.rdd
          .map(x => Map[String, Long](x.getString(0) -> x.getLong(1)))
          .collect
          .map(x => ipCountDict ++= x)
        // serialize scala objects
        val jsonIp = ipCountDict.asJson
        val jsonUri = uriCountDict.asJson
        val report = jsonReport(date, jsonIp, jsonUri)
        val reportJson = report.asJson.noSpaces.toString()
        // write to the file
        file.write(reportJson + "\n")
      }
    }
    file.close()
  }

  def main(args: Array[String]) {
    createReport("access.log.gz")
    spark.close()
  }
}
