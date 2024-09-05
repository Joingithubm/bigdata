package com.zeng.mr.mapper;

import com.zeng.mr.bean.SpeakBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author fanchao
 * @Date 2024/09/05/10:03
 * @Description
 */
public class SpeakMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {

    Text device_id;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, SpeakBean>.Context context) throws IOException, InterruptedException {
        device_id = new Text();
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, SpeakBean>.Context context) throws IOException, InterruptedException {
        device_id = null;
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SpeakBean>.Context context) throws IOException, InterruptedException {

        String trim = value.toString().trim();
        if (StringUtils.isBlank(trim)) {
            return;
        }
        String[] split = trim.split(" ");
        String deviceId = split[1].trim();
        String selfDuration = split[4].trim();
        String thirdDuration = split[5].trim();
        SpeakBean speakBean = new SpeakBean(Long.valueOf(selfDuration), Long.valueOf(thirdDuration), deviceId);
        device_id.set(deviceId);
        context.write(device_id, speakBean);
    }
}
