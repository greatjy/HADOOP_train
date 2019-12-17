package com.alibaba.hadoopdemo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/16 19:58
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    /**
     * map任务把value读进来的行数据，按照指定的分隔符进行拆开。
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
          //把value对应的行数据，按照指定的分隔符进行分开
        String[] words = value.toString().split(" ");
        // 将单词写成key ; 1的形式，剩下的交给reduce来处理
        for(String word : words){
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
