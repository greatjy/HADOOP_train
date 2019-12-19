package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.mrv1;

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

/**
 * @Author: wjy
 * @Date: 2019/12/19 16:41
 */
public class PageStatApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("output/eshop_project/pageview");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);

        job.setJarByClass(PageStatApp.class);
        job.setMapperClass(pageStatMapper.class);
        job.setReducerClass(pageStatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/eshop_project/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }

    static class pageStatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String url = LogParser.parse(value.toString()).get("url");
            if(url != null && !url.equals("-")) {
                String topicId = ContentUtils.getPageId(url);
                context.write(new Text(topicId), ONE);
            }
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
