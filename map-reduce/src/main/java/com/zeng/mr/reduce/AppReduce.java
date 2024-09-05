package com.zeng.mr.reduce;

import com.zeng.mr.bean.PartitionBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * @Author fanchao
 * @Date 2024/09/05/16:51
 * @Description
 */
public class AppReduce extends Reducer<Text, PartitionBean, Text, PartitionBean> {
    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Reducer<Text, PartitionBean, Text, PartitionBean>.Context context) throws IOException, InterruptedException {
        for (PartitionBean value : values) {
            context.write(key, value);
        }
    }
}
