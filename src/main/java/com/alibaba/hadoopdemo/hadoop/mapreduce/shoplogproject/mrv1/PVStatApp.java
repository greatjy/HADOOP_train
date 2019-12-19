package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.mrv1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/18 20:27
 * 第一版本的浏览量的统计，事实上就是log数量的记录数目 Driver类
 */
public class PVStatApp {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        Job job;
        try {
            job = Job.getInstance(configuration);

            //设置执行类
            job.setJarByClass(PVStatApp.class);

            //设置mapper类和reducer类
            job.setMapperClass(PVMapper.class);
            job.setReducerClass(PVReducer.class);

            //设置mapper 和 reducer的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(LongWritable.class);

            //设置输出输入路径
            FileInputFormat.setInputPaths(job, new Path("input/eshop_project/trackinfo_20130721.data"));
            FileOutputFormat.setOutputPath(job, new Path("output/eshop_project"));
            try {
                boolean result = job.waitForCompletion(true);
                System.out.println("this job is "+result);
            } catch (InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            } {

            }


        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    /**
     * 自定义mapper将读进来的数据拆分（默认拆成一行一行的value） 将key 固定 value 1 输出
     * 每一条记录的输出都是一样的，直接再外面定义一个final对象将其输出就可以了
     *
     */


    static class PVMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private final Text PV_COUNT =  new Text("PV_COUNT");
        private final LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             context.write(PV_COUNT, ONE);
        }
    }

    static class PVReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0;
            for(LongWritable value : values) {
                result += value.get();
            }
            context.write(NullWritable.get(), new LongWritable(result));
        }
    }
}
