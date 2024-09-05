package com.zeng.mr;

import com.zeng.mr.bean.PartitionBean;
import com.zeng.mr.mapper.AppMapper;
import com.zeng.mr.partition.AppKeyPartition;
import com.zeng.mr.reduce.AppReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * @Author fanchao
 * @Date 2024/09/05/16:51
 * @Description
 */
public class AppDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "appDriver");
        job.setJarByClass(AppDriver.class);
        job.setPartitionerClass(AppKeyPartition.class);

        job.setMapperClass(AppMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);

        job.setReducerClass(AppReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Iterable.class);
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion(Boolean.TRUE);
        System.exit(b ? 0 : 1);
    }
}
