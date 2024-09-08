package com.zeng.smallfile.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * @Author: fanchao
 * @Date: 2024-09-08 21:16
 * @Description:
 **/
public class CustomRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit split;
    private Configuration conf;
    private Text key;
    private BytesWritable value;
    private Boolean success = Boolean.TRUE;



    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        conf = context.getConfiguration();
        key = new Text();
        value = new BytesWritable();
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!success){
            return false;
        }

        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream open = fs.open(path);
        byte[] bytes = new byte[(int) split.getLength()];

        IOUtils.readFully((InputStream) open, bytes, 0, (int) split.getLength());
        key.set(path.toString());
        value.set(bytes, 0, (int) split.getLength());

        IOUtils.closeStream(open);
        success = Boolean.FALSE;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        key = null;
        value = null;
    }
}
