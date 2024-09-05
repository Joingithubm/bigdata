package com.zeng.mr;

import com.zeng.mr.bean.SpeakBean;
import com.zeng.mr.mapper.SpeakMapper;
import com.zeng.mr.reduce.SpeakReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * @Author fanchao
 * @Date 2024/09/05/10:51
 * @Description
 */
public class SpeakDriver {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "speakDriver");
        job.setJarByClass(SpeakDriver.class);
        job.setNumReduceTasks(2);

        job.setMapperClass(SpeakMapper.class);
        job.setReducerClass(SpeakReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
