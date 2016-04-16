import com.tutorial.utils.SparkCommon
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by koduki on 2016/04/04.
  */
object Example03 {
  case class Person(val name:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Example01")
    val sc = new SparkContext(conf)

    sc.parallelize(Array("Nanoha", "Fate", "Vivio","Nanoha", "Vivio","Nanoha", "Fate"))
      .map(x => Person(x))
      .foreach(println)
    sc.stop()
  }


}
