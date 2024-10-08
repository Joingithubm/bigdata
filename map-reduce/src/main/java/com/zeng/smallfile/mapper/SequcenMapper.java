package com.zeng.smallfile.mapper;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: fanchao
 * @Date: 2024-09-08 21:37
 * @Description:
 **/
public class SequcenMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {


    @Override
    protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
