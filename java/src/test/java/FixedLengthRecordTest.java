/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import cn.orz.pascal.example.saprk.FixedLengthRecordInputFormat;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static org.junit.Assert.*;
import static org.hamcrest.core.Is.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author koduki
 */
public class FixedLengthRecordTest {

    private static JavaSparkContext sc;
    private static final String basedir = "src/test/resources/";

    @BeforeClass
    public static void setUpClass() {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("FixedLengthRecordTest");
        sc = new JavaSparkContext(sparkConf);
    }

    @AfterClass
    public static void tearDownClass() {
        sc.stop();
    }

    private static JavaRDD<String> read(JavaSparkContext sc, File input, int recordLength, int minPartions) {
        String path = input.getAbsoluteFile().toURI().toString().replaceFirst("file:", "");
        sc.hadoopConfiguration().setInt(input.getName() + ".length", recordLength);
        return sc.hadoopFile(path, FixedLengthRecordInputFormat.class, LongWritable.class, BytesWritable.class, minPartions)
                .map(x -> new String(x._2().copyBytes()));
    }

    @Test
    public void readTestWithPartition01() {
        List<String> actual = read(sc, new File(basedir + "testdata"), 10, 1).collect();
        assertThat(actual.size(), is(10));
        assertThat(actual, is(Arrays.asList(
                "001s00000e", "002s00000e", "003s00000e", "004s00000e", "005s00000e",
                "006s00000e", "007s00000e", "008s00000e", "009s00000e", "010s00000e"
        )));
    }

    @Test
    public void readTestWithPartition02() {
        List<String> actual = read(sc, new File(basedir + "testdata"), 10, 2).collect();
        assertThat(actual.size(), is(10));
        assertThat(actual, is(Arrays.asList(
                "001s00000e", "002s00000e", "003s00000e", "004s00000e", "005s00000e",
                "006s00000e", "007s00000e", "008s00000e", "009s00000e", "010s00000e"
        )));
    }

    @Test
    public void readTestWithPartition03() {
        List<String> actual = read(sc, new File(basedir + "testdata"), 10, 3).collect();
        assertThat(actual.size(), is(10));
        assertThat(actual, is(Arrays.asList(
                "001s00000e", "002s00000e", "003s00000e", "004s00000e", "005s00000e",
                "006s00000e", "007s00000e", "008s00000e", "009s00000e", "010s00000e"
        )));
    }

    @Test
    public void readTestWithPartition11() {
        List<String> actual = read(sc, new File(basedir + "testdata"), 10, 11).collect();
        assertThat(actual.size(), is(10));
        assertThat(actual, is(Arrays.asList(
                "001s00000e", "002s00000e", "003s00000e", "004s00000e", "005s00000e",
                "006s00000e", "007s00000e", "008s00000e", "009s00000e", "010s00000e"
        )));
    }

}
