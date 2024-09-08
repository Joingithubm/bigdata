package com.zeng.smallfile.output;

import com.zeng.smallfile.writer.CustomWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: fanchao
 * @Date: 2024-09-08 22:13
 * @Description:
 **/
public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream barOut = fs.create(new Path("/tmp/mr/output/bar/bar.log"));
        FSDataOutputStream karOut = fs.create(new Path("/tmp/mr/output/kar/kar.log"));
        return new CustomWriter(karOut, barOut);
    }
}
