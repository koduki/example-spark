import com.tutorial.utils.SparkCommon
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

/**
  * Created by koduki on 2016/04/04.
  */
object Example01 {
  def main(args: Array[String]): Unit = {
    val sc = SparkCommon.sparkContext
    val input = sc.textFile("src/main/resources/KEN_All_ROME.CSV")
    val count = input
      .map { line => line.replace("\"", "").split(",") }
      .map { xs =>
        xs(6) match {
          case "IKANIKEISAIGANAIBAAI" => Array(xs(0), xs(4), xs(5))
          case _ => Array(xs(0), xs(4), xs(5), xs(6))
        }
      }
      .filter(xs => xs(1) == "FUKUOKA KEN")
      .map(xs => xs(2))
      .saveAsTextFile("output.txt")

    println("Fukuoka ken:" + count);
    sc.stop()
  }
}
