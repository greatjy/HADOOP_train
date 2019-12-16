package com.alibaba.hadoopdemo.hadoop.hdfs;

/**
 * @Author: wjy
 * @Date: 2019/12/15 21:10
 */
public interface WordcountMapper
{
    /**
     *  将输入的每一行进行处理之后，结果放在context缓存中
     * @param line
     * @param context
     */
    public void map(String line, HDFSWCContext context);
}
