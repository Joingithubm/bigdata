package com.zeng.mr.mapper;

import com.zeng.mr.bean.PartitionBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Objects;

/**
 * @Author fanchao
 * @Date 2024/09/05/16:51
 * @Description
 */
public class AppMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {

    Text appType;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, PartitionBean>.Context context) throws IOException, InterruptedException {
        appType = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PartitionBean>.Context context) throws IOException, InterruptedException {
        if (Objects.isNull(value) || StringUtils.isBlank(value.toString())){
            return;
        }

        String line = value.toString().trim();
        String[] split = line.split(" ");
        PartitionBean partitionBean = new PartitionBean(split[0].trim(), split[1].trim(), split[2].trim(), split[3].trim()
                , Long.valueOf(split[4].trim()), Long.valueOf(split[5].trim()), Long.valueOf(split[6]));
        appType.set(split[1].trim());
        context.write(appType, partitionBean);
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, PartitionBean>.Context context) throws IOException, InterruptedException {
        appType = null;
    }
}
