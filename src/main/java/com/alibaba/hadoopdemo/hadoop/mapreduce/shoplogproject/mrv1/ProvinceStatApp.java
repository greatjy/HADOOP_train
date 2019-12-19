package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.mrv1;

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
import java.util.Map;

/**
 * @Author: wjy
 * @Date: 2019/12/19 15:45
 *  省份浏览量统计量
 */
public class ProvinceStatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("output/eshop_project/province");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);

        job.setJarByClass(ProvinceStatApp.class);
        job.setMapperClass(ProvinceMapper.class);
        job.setReducerClass(ProvinceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/eshop_project/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, new Path("output/eshop_project/province"));

        boolean result = job.waitForCompletion(true);
        System.out.println(result);

    }

    static class ProvinceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> info = LogParser.parse(value.toString());
            String ip = "ip";
            String nonParse = "-";
            if(!info.get(ip).equals(nonParse)){
                String province = info.get("province");
                if(province != null) {
                    context.write(new Text(info.get("province")), ONE);
                }
                else{
                    context.write(new Text("province is null"), ONE);
                }
            }
            else {
                context.write(new Text(nonParse), ONE);
            }
        }
    }

    static class ProvinceReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
