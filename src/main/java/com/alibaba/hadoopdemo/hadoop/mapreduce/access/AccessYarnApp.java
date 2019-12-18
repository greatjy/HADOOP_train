package com.alibaba.hadoopdemo.hadoop.mapreduce.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/17 20:34
 */
public class AccessYarnApp {

    /**
     * Driver端代码相对固定不需要做太多改动
     * @param args  args[0]表示输入路径  args[1]表示输出路径
     */
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(configuration);
            //设置要处理的类
            job.setJarByClass(AccessYarnApp.class);

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
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean result = AccessLocalApp.getResult(job);

        System.out.println(result + "result");

    }
}
