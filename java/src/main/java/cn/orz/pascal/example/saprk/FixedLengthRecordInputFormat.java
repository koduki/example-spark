/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author koduki
 */
public class FixedLengthRecordInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split, JobConf jc, Reporter rprtr) throws IOException {
        return new FixedLengthRecordReader((FileSplit) split, jc);
    }

}
