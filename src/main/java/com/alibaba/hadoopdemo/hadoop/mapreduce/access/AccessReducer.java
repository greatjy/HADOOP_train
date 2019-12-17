package com.alibaba.hadoopdemo.hadoop.mapreduce.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wjy
 * @Date: 2019/12/17 19:26
 */
public class AccessReducer extends Reducer<Text, Access, NullWritable, Access>
{
    /**
     *
     * @param key 手机号
     * @param values 最终得到的数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long uploads = 0;
        long downloads = 0;
        long totals = 0;
        for(Access access : values){
            uploads += access.getUpwardFlow();
            downloads += access.getDownloadFlow();
            totals += access.getTotalFlow();
        }
        Access resultAccess = new Access(key.toString(), uploads, downloads);
        context.write(NullWritable.get(), resultAccess);
    }
}
