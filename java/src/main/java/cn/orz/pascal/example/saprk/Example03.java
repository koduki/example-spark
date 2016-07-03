/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

/**
 *
 * @author koduki
 */
public class Example03 {

    static class Person implements Serializable {

        String name;

        public Person(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Person(" + name + ")";
        }

    }

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Example01");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        sc.parallelize(Arrays.asList(
                new Tuple3<>("東京", "製造", 100),
                new Tuple3<>("福岡", "営業", 140),
                new Tuple3<>("福岡", "製造", 10)))
                .mapToPair(xs -> new Tuple2(xs._1(), xs))
                .groupByKey()
                .map(xs -> {
                    // 型推論が何故かうまくいかない
                    Tuple2<String, Iterable<Tuple3<String, String, Integer>>> item = (Tuple2<String, Iterable<Tuple3<String, String, Integer>>>) xs;
                    Stream<Tuple3<String, String, Integer>> stream = StreamSupport.stream(item._2().spliterator(), true);
                    int sum = stream.collect(Collectors.summingInt(x -> x._3()));

                    return new Tuple2<>(item._1(), sum);
                })
                .foreach(x -> System.out.println(x));

        sc.stop();
    }

    static String fizzBuzz(Integer x) {
        if (x % 15 == 0) {
            return "FizzBuzz";
        } else if (x % 5 == 0) {
            return "Fizz";
        } else if (x % 3 == 0) {
            return "Buzz";
        } else {
            return x.toString();
        }
    }
}
