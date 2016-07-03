/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.orz.pascal.example.saprk;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import scala.Tuple2;

/**
 *
 * @author koduki
 */
public class FixedLengthRecordReader implements RecordReader<LongWritable, BytesWritable> {

    private final int recordLength;
    private final FSDataInputStream input;
    private long start;
    private long pos;
    private long end;

    public FixedLengthRecordReader(FileSplit split, Configuration job) throws IOException {
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);

        this.recordLength = job.getInt(file.getName() + ".length", 80);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        this.input = fs.open(file);

        int fraction = (int) (this.start % this.recordLength);
        skip(fraction);

        this.pos = this.start;
    }

    @Override
    public boolean next(LongWritable key, BytesWritable value) throws IOException {
        if (this.pos < this.end) {
            key.set(this.pos);

            Tuple2<Boolean, ByteBuffer> record = readLine(this.input, this.recordLength);
            if (record._1()) {
                setValue(record._2(), value);
            }
            return record._1();
        }
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public float getProgress() throws IOException {
        if (this.start == this.end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (this.pos - this.start) / (float) (this.end = this.start));
        }
    }

    @Override
    public void close() throws IOException {
        if (this.input != null) {
            this.input.close();
        }
    }

    private void skip(int fraction) throws IOException {
        this.start -= fraction;
        this.input.seek(this.start);
        if (fraction != 0) {
            Tuple2<Boolean, ByteBuffer> record = readLine(this.input, this.recordLength);
            this.start += record._2().array().length;
        }
    }

    private Tuple2<Boolean, ByteBuffer> readLine(FSDataInputStream in, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(this.recordLength);
        boolean result = _readLine(in, length, buffer);
        return new Tuple2<>(result, buffer);
    }

    private boolean _readLine(FSDataInputStream in, int length, ByteBuffer result) throws IOException {
        byte[] buff = new byte[length];
        int newSize = in.read(buff);
        if (newSize <= 0) {
            return false;
        }

        if (length != newSize) {
            byte[] tmp = trim(buff, newSize);
            result.put(tmp);
            _readLine(in, length - newSize, result);
        } else {
            result.put(buff);
        }

        return true;
    }

    private byte[] trim(byte[] buff, int length) {
        byte[] tmp = new byte[length];
        System.arraycopy(buff, 0, tmp, 0, tmp.length);
        return tmp;
    }

    private void setValue(ByteBuffer in, BytesWritable value) {
        byte[] data = in.array();
        this.pos += data.length;
        value.set(new BytesWritable(data));
    }

}
