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

            //设置mapper 和 reducer的输出
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Access.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Access.class);

            //设置输入输出的路径
            FileInputFormat.setInputPaths(job, new Path("input/access/access.log"));
            FileOutputFormat.setOutputPath(job, new Path("ouput/access/"));

        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean result = false;

        try {
            result = job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println(result + "result");

    }
}
