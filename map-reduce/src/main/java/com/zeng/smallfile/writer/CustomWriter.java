package com.zeng.smallfile.writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author: fanchao
 * @Date: 2024-09-08 22:15
 * @Description:
 **/
public class CustomWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream karOut;
    private FSDataOutputStream barOut;

    public CustomWriter(FSDataOutputStream karOut, FSDataOutputStream barOut) {
        this.karOut = karOut;
        this.barOut = barOut;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("bar")) {
            barOut.write(key.getBytes());
            barOut.write("\r\n".getBytes());
        }
        if (key.toString().contains("kar")){
            karOut.write(key.getBytes());
            karOut.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(karOut);
        IOUtils.closeStream(barOut);
    }
}
