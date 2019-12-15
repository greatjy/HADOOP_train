package com.alibaba.hadoopDemo.hadoop.hdfs;

/**
 * @Author: wjy
 * @Date: 2019/12/15 21:12
 */
public class WordCountMapperImpl implements WordcountMapper {

    /**
     * 实现业务逻辑
     * @param line
     * @param context
     */
    @Override
    public void map(String line, HDFSWCContext context) {
         String[] words = line.split(" ");
         for(String word : words){
             if(context.getCacheMap().containsKey(word)){
                 int count = Integer.parseInt(context.getCache(word).toString());
                 count = count + 1;
                 context.setCacheMap(word, count);
             }
             else{
                 context.setCacheMap(word, "1");
             }
         }
    }
}
