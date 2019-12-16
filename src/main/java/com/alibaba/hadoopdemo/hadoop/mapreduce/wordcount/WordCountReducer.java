package com.alibaba.hadoopdemo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: wjy
 * @Date: 2019/12/16 20:43
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    /**
     * 将shuffle之后的key values 进行合并。 map的输出到reduce 按照相同的key 分发到一个reduce上面进行执行。
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            count += iterator.next().get();
        }
        context.write(key, new IntWritable(count));
    }
}
