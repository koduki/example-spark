/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author koduki
 */
public class Example01 {

    public static void main(String[] args) throws IOException {
        String tmpDir = "target/output_tmp";
        String outputFile = "target/output.txt";
        
        // clear
        FileUtil.fullyDelete(new File(tmpDir));
        FileUtil.fullyDelete(new File(outputFile));

        // init spark config
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // create RDD
        JavaRDD rdd = sc.parallelize(Arrays.asList("a", "b", "c")).repartition(3);
        
        // save as Hadoop file format
        rdd.saveAsTextFile(tmpDir);
        
        // merge as Plain file
        FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
        FileUtil.copyMerge(hdfs, new Path(tmpDir), hdfs, new Path(outputFile), false, sc.hadoopConfiguration(), null);
    }
}
