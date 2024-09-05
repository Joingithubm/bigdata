package com.zeng.mr.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author fanchao
 * @Date 2024/09/05/16:53
 * @Description
 */
public class PartitionBean implements Writable {

    private String number;
    private String deviceId;
    private String appKey;
    private String ip;
    private Long selfDuration;
    private Long thirdDuration;
    private Long status;


    public PartitionBean() {
    }

    public PartitionBean(String number, String deviceId, String appKey, String ip, Long selfDuration, Long thirdDuration, Long status) {
        this.number = number;
        this.deviceId = deviceId;
        this.appKey = appKey;
        this.ip = ip;
        this.selfDuration = selfDuration;
        this.thirdDuration = thirdDuration;
        this.status = status;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(number);
        out.writeUTF(deviceId);
        out.writeUTF(appKey);
        out.writeUTF(ip);
        out.writeLong(selfDuration);
        out.writeLong(thirdDuration);
        out.writeLong(status);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.number = in.readUTF();
        this.deviceId = in.readUTF();
        this.appKey = in.readUTF();
        this.ip = in.readUTF();
        this.selfDuration = in.readLong();
        this.thirdDuration = in.readLong();
        this.status = in.readLong();
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdDuration() {
        return thirdDuration;
    }

    public void setThirdDuration(Long thirdDuration) {
        this.thirdDuration = thirdDuration;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "PartitionBean{" +
                "number='" + number + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", appKey='" + appKey + '\'' +
                ", ip='" + ip + '\'' +
                ", selfDuration=" + selfDuration +
                ", thirdDuration=" + thirdDuration +
                ", status=" + status +
                '}';
    }
}
