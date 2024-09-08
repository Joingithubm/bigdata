package com.zeng.smallfile;

import com.zeng.smallfile.mapper.CustomOutputMapper;
import com.zeng.smallfile.output.CustomOutputFormat;
import com.zeng.smallfile.output.CustomOutputReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: fanchao
 * @Date: 2024-09-08 22:26
 * @Description:
 **/
public class CustomOutputDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CustomOutputDriver");
        job.setJarByClass(CustomOutputDriver.class);

        job.setMapperClass(CustomOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(CustomOutputReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(CustomOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
