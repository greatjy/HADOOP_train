package com.alibaba.hadoopdemo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: wjy
 * @Date: 2019/12/16 20:59
 * mapreduce word count 客户端程序
 */
public class WordCountApp {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.18.136:8020");
        System.setProperty("HADOOP_USER_NAME", "root");

        Job job;
        try {

            //创建一个job，封装和设置job对应的参数【运行的主类，运行的map，运行的reduce】
            job = Job.getInstance(configuration);
            job.setJarByClass(WordCountApp.class);

            // 设置自定义的mapper 和 reducer
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            //设置map的输出类型 也就是reduce的输入类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //需要得到作业输入文件和输出文件的路径,输入路径可以有多个
            Path outputPath = new Path("/wordcount/output");
            FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
            FileOutputFormat.setOutputPath(job, outputPath);

            //如果输出目录存在，则先递归删除这个目录
            FileSystem fileSystem = null;

            try {
                fileSystem = FileSystem.get(new URI("hdfs://192.168.18.136:8020"), configuration, "root");
                if(fileSystem.exists(outputPath)){
                    fileSystem.delete(outputPath, true);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //提交job
            boolean result = false;
            try {
                // Submit the job to the cluster and wait for it to finish. verbose print the progress to the user
                result = job.waitForCompletion(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            System.exit(result ? 0 : -1);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
