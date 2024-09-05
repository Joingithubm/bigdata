package com.zeng.mr.partition;

import com.zeng.mr.bean.PartitionBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @Author fanchao
 * @Date 2024/09/05/17:32
 * @Description
 */
public class AppKeyPartition extends Partitioner<Text, PartitionBean> {

    @Override
    public int getPartition(Text key, PartitionBean value, int numPartitions) {

        if (key.toString().startsWith("0015")) {
            return 1;
        }
        if (key.toString().startsWith("0019")){
            return 2;
        }
        return 0;
    }

}
