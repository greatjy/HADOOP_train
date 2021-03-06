package com.alibaba.hadoopdemo.hadoop.mapreduce.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/17 20:34
 */
public class AccessLocalApp {

    //Driver 端的代码相对固定，不需要做太多的更改
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(configuration);
            //设置要处理的类
            job.setJarByClass(AccessLocalApp.class);

            //设置mapper 和 reducer的类
            job.setMapperClass(AccessMapper.class);
            job.setReducerClass(AccessReducer.class);

            //按照自定义需求设置partition
            job.setPartitionerClass(AccessPartitioner.class);
            job.setNumReduceTasks(4);

            //设置mapper 和 reducer的输出
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Access.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Access.class);

            //设置输入输出的路径
            FileInputFormat.setInputPaths(job, new Path("input/access/access.log"));
            FileOutputFormat.setOutputPath(job, new Path("output/access/"));

        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean result = false;

        result = getResult(job);

        System.out.println(result + "result");

    }

    public static boolean getResult(Job job) {
        boolean result = false;
        try {
            if (job != null) {
                result = job.waitForCompletion(true);
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }
}
