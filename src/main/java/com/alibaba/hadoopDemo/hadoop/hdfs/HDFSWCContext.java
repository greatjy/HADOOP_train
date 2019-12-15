package com.alibaba.hadoopDemo.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wjy
 * @Date: 2019/12/15 20:53
 * 自定义上下文
 */
public class HDFSWCContext {
    private Map<Object, Object> cacheMap = new HashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    /**
     * 将数据写入缓存 单词：次数 key value
     *
     * @param key   单词
     * @param value 词频
     */
    public void writeToCache(Object key, Object value) {
        cacheMap.put(key, value);
    }

    /**
     * 从缓存中获取值
     *
     * @param key 单词
     * @return
     */
    public Object readFromCache(Object key) {
        return cacheMap.get(key);
    }

}
