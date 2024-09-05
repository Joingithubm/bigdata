package com.zeng.mr.reduce;

import com.zeng.mr.bean.SpeakBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author fanchao
 * @Date 2024/09/05/10:41
 * @Description
 */
public class SpeakReduce extends Reducer<Text, SpeakBean, Text, SpeakBean> {

    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Reducer<Text, SpeakBean, Text, SpeakBean>.Context context) throws IOException, InterruptedException {
        Iterator<SpeakBean> iterator = values.iterator();
        SpeakBean speakBean = new SpeakBean(0l, 0l, key.toString());
        while (iterator.hasNext()) {
            SpeakBean next = iterator.next();
            speakBean.setSelfDuration(speakBean.getSelfDuration() + next.getSelfDuration());
            speakBean.setThirdPartDuration(speakBean.getThirdPartDuration() + next.getThirdPartDuration());
            speakBean.setSumDuration(speakBean.getSumDuration() + speakBean.getSelfDuration() + speakBean.getThirdPartDuration());
        }
        context.write(key, speakBean);
    }
}
