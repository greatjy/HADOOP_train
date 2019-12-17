package com.alibaba.hadoopdemo.hadoop.mapreduce.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: wjy
 * @Date: 2019/12/17 22:24
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    /**
     *
     * @param text  手机号
     * @param access  统计结果
     * @param numPartitions  分区数
     * @return
     */
    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if (text.toString().startsWith("13")){
            return 0;
        }
        else if (text.toString().startsWith("15")) {
            return 1;
        }
        else if (text.toString().startsWith("18")) {
            return 2;
        }
        else {
            return 3;
        }
    }
}
