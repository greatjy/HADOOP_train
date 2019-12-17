package com.alibaba.hadoopdemo.hadoop.mapreduce.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/17 19:07
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    /**
     * 自定义Mapper类，将所需要的数据提取出来，做一些适当的转换。
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");
        // 从切分的数据中得到对应位置的数据
        if (lines.length < 3) {
            Access access = new Access("error", 0, 0);
            context.write(new Text(lines[0]), access);
        } else {
            String phoneNumber = lines[1];
            long upwardFlow = Long.parseLong(lines[lines.length - 3]);
            long downloadFlow = Long.parseLong(lines[lines.length - 2]);
            Access access = new Access(phoneNumber, upwardFlow, downloadFlow);
            context.write(new Text(phoneNumber), access);
        }
    }
}
