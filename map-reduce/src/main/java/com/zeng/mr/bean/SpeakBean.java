package com.zeng.mr.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author fanchao
 * @Date 2024/09/05/10:13
 * @Description
 */
public class SpeakBean implements Writable{

    private Long selfDuration;
    private Long thirdPartDuration;
    private String deviceId;
    private Long sumDuration;


    public SpeakBean() {
    }

    public SpeakBean(Long selfDuration, Long thirdPartDuration, String deviceId) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = selfDuration + thirdPartDuration;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceId);
        out.writeLong(sumDuration);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDuration = in.readLong();
        this.thirdPartDuration = in.readLong();
        this.deviceId = in.readUTF();
        this.sumDuration = in.readLong();
    }


    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }

    @Override
    public String toString() {
        return "SpeakBean{" +
                "selfDuration=" + selfDuration +
                ", thirdPartDuration=" + thirdPartDuration +
                ", deviceId='" + deviceId + '\'' +
                ", sumDuration=" + sumDuration +
                '}';
    }
}
