import com.tutorial.utils.SparkCommon

/**
  * Created by koduki on 2016/04/04.
  */
object Example02 {
  def main(args: Array[String]): Unit = {
    val sc = SparkCommon.sparkContext

    println("start calculate")

    val s = System.currentTimeMillis();
    val count = sc.parallelize(Range(0, 10))
      .map { x =>
        Thread.sleep(1000);
        x
      }
      .count()

    println("count:" + count);
    println("calculate time: " + (System.currentTimeMillis() - s) + " ms")

    sc.stop()
  }
}
