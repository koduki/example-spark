/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author koduki
 */
public class Example04 {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Example04");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String basedir = "src/main/resources/";
        read(sc, new File(basedir + "testdata"), 10, 1).foreach((x) -> System.out.println("01:" + x));
        read(sc, new File(basedir + "testdata"), 10, 2).foreach((x) -> System.out.println("02:" + x));
        read(sc, new File(basedir + "testdata"), 10, 3).foreach((x) -> System.out.println("03:" + x));
        read(sc, new File(basedir + "testdata"), 10, 11).foreach((x) -> System.out.println("11:" + x));

        sc.stop();
    }

    private static JavaRDD<String> read(JavaSparkContext sc, File input, int recordLength, int minPartions) {
        String path = input.getAbsoluteFile().toURI().toString().replaceFirst("file:", "");

        System.out.println(String.format("minPartions=%d, recordLength=%d, path=%s", minPartions, recordLength, path));
        sc.hadoopConfiguration().setInt(input.getName() + ".length", recordLength);
        return sc.hadoopFile(path, FixedLenghthRecordInputFormat.class, LongWritable.class, BytesWritable.class, minPartions)
                .map(x -> new String(x._2().copyBytes()));
    }

}
