package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.mrv2;

import com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.utills.ContentUtils;
import com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.utills.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wjy
 * @Date: 2019/12/19 16:41
 */
public class Pagev2StatApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("output/v2/pageview");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);

        job.setJarByClass(Pagev2StatApp.class);
        job.setMapperClass(pageStatMapper.class);
        job.setReducerClass(pageStatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/etl"));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }

    static class pageStatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> info = new HashMap<>();
            info = LogParser.parse(value.toString());
            context.write(new Text(info.get("pageId")), ONE);
        }
    }

    static class pageStatReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0;
            for(LongWritable value : values){
                result += value.get();
            }
            context.write(key, new LongWritable(result));
        }
    }


}
