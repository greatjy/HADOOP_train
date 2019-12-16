package com.alibaba.hadoopdemo.hadoop;

import java.util.Properties;

/**
 * @Author: wjy
 * @Date: 2019/12/15 21:47
 * 读取属性配置文件
 */
public class ParameterUtils {
    private static Properties properties = new Properties();
    static {
        try{
            properties.load(ParameterUtils.class.getClassLoader().getResourceAsStream("wordcount.properties"));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Properties getProperties() throws Exception {
        return properties;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getProperties().getProperty("USER"));
    }
}
