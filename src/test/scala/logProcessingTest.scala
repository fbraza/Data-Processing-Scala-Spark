import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import LogDataProject._
import org.apache.spark.sql._
import scala.util.matching.Regex

class PatternMatchingSpec extends AnyFlatSpec with should.Matchers {

  val datePattern = "(\\d{2}/\\w{3}/\\d{4})"
  val ipPattern = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})"
  val uriPattern = """"[A-Z]+ (\/.+) HTTP\/\d\.\d""""

  def doesMatch(pattern: String, data: String): Boolean = {
    val regexp = pattern.r
    data match {
      case regexp(group) => true
      case _             => false
    }
  }

  "Date patterns" should "match the following data" in {
    assert(doesMatch(datePattern, "12/Dec/2015"))
    assert(doesMatch(datePattern, "25/Feb/2016"))
    assert(doesMatch(datePattern, "11/Jun/2015"))
  }

  "Date patterns" should "not match the following data" in {
    assert(!doesMatch(datePattern, "12/December/2015:18:25:11 +0100"))
    assert(!doesMatch(datePattern, "25/2016/Feb:22:11:00"))
    assert(!doesMatch(datePattern, "11-Jun-2015:09:44:11"))
  }

  "Ip patterns" should "match the following data" in {
    assert(doesMatch(ipPattern, "109.169.248.247"))
    assert(doesMatch(ipPattern, "127.0.0.0"))
    assert(doesMatch(ipPattern, "46.72.177.4"))
  }

  "Ip patterns" should "not match the following data" in {
    assert(!doesMatch(ipPattern, "1192.1178.10.10"))
    assert(!doesMatch(ipPattern, "03.xxx.10.10"))
    assert(!doesMatch(ipPattern, "localhost"))
  }

  "URI patterns" should "match the following data" in {
    assert(doesMatch(uriPattern, """"GET /administrator/ HTTP/1.1""""))
    assert(
      doesMatch(uriPattern, """"POST /administrator/index.php HTTP/1.1"""")
    )
    assert(
      doesMatch(uriPattern, """"DELETE /administrator/index.php HTTP/1.1"""")
    )
  }

  "URI patterns" should "not match the following data" in {
    assert(!doesMatch(uriPattern, """"GET-/administrator/-HTTP/1.1""""))
    assert(
      !doesMatch(uriPattern, """"POST administrator/index.php HTTP/1.1"""")
    )
    assert(
      !doesMatch(uriPattern, """"DELETE /administrator/index.php HTTP/10.1"""")
    )
  }
}

class ExploratoryAnalysisSpec extends AnyFlatSpec with should.Matchers {

  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  val log = Log("access.log.gz")
  val raw = log.rawData
  val clean = log.cleanData
  val report = log.reportData

  raw.cache()
  clean.cache()
  report.cache()

  "Number of lines in our log file" should """be equal to 2660050 (raw) & 
  2660049 (clean) & xxxxx (report)""" in {
    raw.count() should be(2660050)
    clean.count() should be(2660049)
    report.count() should be(27378)
  }

  "Number of non-matching uri" should "be equal to 50024" in {
    clean
      .select("*")
      .where(functions.col("Uri") === "")
      .count() should be(50024)
  }

  "Number of unique URI with number of connections superior to 20000" should "be 13" in {
    clean
      .groupBy("Date", "Uri")
      .count()
      .select("Uri", "count")
      .where(functions.col("count") > 20000)
      .distinct()
      .count() should be(13)
  }
}
