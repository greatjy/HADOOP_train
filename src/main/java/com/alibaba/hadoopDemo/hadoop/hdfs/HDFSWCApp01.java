package com.alibaba.hadoopDemo.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author: wjy
 * @Date: 2019/12/15 19:42
 * 使用HDFS API完成wordcount统计
 *
 * 需求：统计HDFS文件系统上的wc，输出到HDFS src和dest都是hdfs
 *
 * 功能拆解：1.读取HDFS上的文件   ----HDFS API
 *           2.进行词频统计：对读取进来的文件的每一行都进行处理【按照分隔符分割，分开以后累加数字】  ----抽取一个Map
 *           3.将处理结果缓存。  -----抽取一个contest
 *           4.结果输出到HDFS  ----- HDFS API
 */
public class HDFSWCApp01 {

    public static void main(String[] args) {
        // 1）配置HDFS客户端，获取要操作的hdfs文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://192.168.18.136:8020"), configuration, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        //2 从hdfs文件系统上读取文件
        Path input = new Path("/test_hadoop/test_wordcount.txt");
        RemoteIterator<LocatedFileStatus> iterator = null;
        try {
             iterator = fileSystem.listFiles(input, false);
             while(iterator.hasNext()) {
                 LocatedFileStatus fileStatus = iterator.next();
                 FSDataInputStream  in = fileSystem.open(fileStatus.getPath());
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                 String line = "";
                 while((line = reader.readLine()) != null){
                     // 进行词频处理

                 }
                 reader.close();
                 in.close();
             }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //@R=ToDO 3  将结果缓存起来  Map
        Map<Object, Object> contextMap = new HashMap<>();


        //4 将结果保存到hdfs文件系统上
        Path output = new Path("/test_hadoop/output/");
        try {
            FSDataOutputStream out = fileSystem.create(new Path(output,"word_count.txt"));
            Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
            for(Map.Entry entry : entries){
                out.writeChars(entry.getKey()+" : "+entry.getValue()+"\n");
            }
            out.close();
            fileSystem.close();
            System.out.println("词频统计运行完成");
            // 将保存的map信息写道文件中，返回。
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
