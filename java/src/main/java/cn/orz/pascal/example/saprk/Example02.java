/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author koduki
 */
public class Example02 {

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
                new Tuple2<>("Nanoha", 19), 
                new Tuple2<>("Fate", 19),
                new Tuple2<>("Vivio", 9)))
                .filter(x -> x._2() < 10)
                .map(x -> "Magical girl " + x._1())
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
