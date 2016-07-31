/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import static org.apache.spark.sql.functions.broadcast;
import scala.runtime.AbstractFunction1;

/**
 *
 * @author koduki
 */
public class Example013 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Example03").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Tuple2<Integer, String>> left = createPairRdd(sc, "Left", 1_000_000);
        JavaPairRDD<Integer, Tuple2<Integer, String>> right = createPairRdd(sc, "Right", 100);

        bench("Map-side_join(closure)", 5, () -> {
            Set<Integer> set = new HashSet<>(right.map(x -> x._1).collect());
            left.map(x -> set.contains(x._1) ? x._2 : "NONE")
                    .count();
        });

        bench("Map-side_join(broadcast)", 5, () -> {
            Broadcast<Set<Integer>> set = sc.broadcast(new HashSet<>(right.map(x -> x._1).collect()));
            left.map(x -> set.value().contains(x._1) ? x._2 : "NONE")
                    .count();
        });

        bench("outer_join(pair)", 5, () -> {
            left.leftOuterJoin(right)
                    .map(xs -> (xs._2._2.isPresent()) ? xs._2._1._2 : "NONE")
                    .count();
        });

        bench("outer_join(df)", 5, () -> {
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame leftDf = sqlContext.createDataFrame(left.map(x -> new Item(x._2._1, x._2._2)), Item.class);
            DataFrame rightDf = sqlContext.createDataFrame(right.map(x -> new Item(x._2._1, x._2._2)), Item.class);

            leftDf.as("left")
                    .join(rightDf.as("right"), leftDf.col("id").equalTo(rightDf.col("id")), "leftouter")
                    .javaRDD()
                    .map(xs -> (xs.get(2) != null) ? xs.get(1) : "NONE")
                    .count();
        });

        bench("outer_join(df:bloadcast)", 5, () -> {
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame leftDf = sqlContext.createDataFrame(left.map(x -> new Item(x._2._1, x._2._2)), Item.class);
            DataFrame rightDf = sqlContext.createDataFrame(right.map(x -> new Item(x._2._1, x._2._2)), Item.class);

            leftDf.as("left")
                    .join(broadcast(rightDf.as("right")), leftDf.col("id").equalTo(rightDf.col("id")), "leftouter")
                    .javaRDD()
                    .map(xs -> (xs.get(2) != null) ? xs.get(1) : "NONE")
                    .count();
        });

//        bench("outer_join(df:native)", 5, () -> {
//            SQLContext sqlContext = new SQLContext(sc);
//            DataFrame leftDf = sqlContext.createDataFrame(left.map(x -> new Item(x._2._1, x._2._2)), Item.class);
//            DataFrame rightDf = sqlContext.createDataFrame(right.map(x -> new Item(x._2._1, x._2._2)), Item.class);
//
//            leftDf.as("left")
//                    .join(broadcast(rightDf.as("right")), leftDf.col("id").equalTo(rightDf.col("id")), "leftouter")
//                    .map(new AbstractFunction1<Row, String>() {
//                        @Override
//                        public String apply(Row t1) {
//                            return (t1.getAs(2) != null ? t1.getAs(1) : "NONE");
//                        }
//                    }, scala.reflect.ClassManifestFactory.fromClass(String.class))
//                    .count();
//        });
    }

    private static void bench(String msg, int count, Runnable callback) {
        List<Long> counts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long s = System.nanoTime();
            callback.run();
            long e = System.nanoTime();
            counts.add(e - s);
        }
        counts.stream().forEach(x -> System.out.println("bench::" + msg + ":\t" + x / 1000 / 1000 + " ms"));
    }

    private static JavaPairRDD<Integer, Tuple2<Integer, String>> createPairRdd(JavaSparkContext sc, String msg, int length) {
        List<Tuple2<Integer, String>> src = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            src.add(new Tuple2<>(i, msg + i));
        }
        JavaPairRDD<Integer, Tuple2<Integer, String>> data = sc
                .parallelize(src)
                .mapToPair(x -> new Tuple2<>(x._1, x));
        return data;
    }
}
