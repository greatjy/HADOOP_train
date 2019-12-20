package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.mrv2;

import com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.mrv1.ProvinceStatApp;
import com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.utills.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: wjy
 * @Date: 2019/12/19 17:08
 */
public class ETLApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("input/etl");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);

        job.setJarByClass(ETLApp.class);
        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path("input/eshop_project/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }

    static class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private LongWritable ONE = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> info = LogParser.parse(log);
            String ip = info.get("ip");
            String url = info.get("url");
            String time = info.get("time");
            String country = info.get("country");
            String province = info.get("province");
            String city = info.get("city");
            String pageId = info.get("pageId");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ip).append("\t");
            stringBuilder.append(url).append("\t");
            stringBuilder.append(time).append("\t");
            stringBuilder.append(country).append("\t");
            stringBuilder.append(province).append("\t");
            stringBuilder.append(city).append("\t");
            stringBuilder.append(pageId);

            context.write(NullWritable.get(), new Text(stringBuilder.toString()));


        }
    }
}
