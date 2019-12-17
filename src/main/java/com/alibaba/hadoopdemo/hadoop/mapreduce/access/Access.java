package com.alibaba.hadoopdemo.hadoop.mapreduce.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/17 17:27
 * 自定义复杂数据类型
 */
public class Access implements Writable {
    private String phoneNumber;
    private long upwardFlow;
    private long downloadFlow;
    private long totalFlow;

    public Access(){}

    public Access(String phoneNumber, long upwardFlow, long downloadFlow) {
        this.phoneNumber = phoneNumber;
        this.upwardFlow = upwardFlow;
        this.downloadFlow = downloadFlow;
        this.totalFlow = upwardFlow + downloadFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
         out.writeUTF(phoneNumber);
         out.writeLong(upwardFlow);
         out.writeLong(downloadFlow);
         out.writeLong(totalFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phoneNumber = in.readUTF();
        this.upwardFlow = in.readLong();
        this.downloadFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public long getUpwardFlow() {
        return upwardFlow;
    }

    public void setUpwardFlow(long upwardFlow) {
        this.upwardFlow = upwardFlow;
    }

    public long getDownloadFlow() {
        return downloadFlow;
    }

    public void setDownloadFlow(long downloadFlow) {
        this.downloadFlow = downloadFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    @Override
    public String toString() {
        return "Access{" + "phoneNumber='" + phoneNumber + '\'' +
                ", upwardFlow=" + upwardFlow + ", downloadFlow=" + downloadFlow + ", totalFlow=" + totalFlow + '}';
    }


}
