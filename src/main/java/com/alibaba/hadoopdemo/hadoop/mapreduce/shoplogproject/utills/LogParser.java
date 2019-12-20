package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.utills;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    public static Map<String, String> parse(String log) {
        Map<String,String> info = new HashMap<>();
        String url = "-";
        String ip = "-";
        String country = "-";
        String province = "-";
        String city = "-";
        String time = "-";
        String pageId = "-";
        if(!(log == null || log.equals(""))){
            String[] splitData = log.split("\t");
            ip = splitData[0];
            url = splitData[1];
            time = splitData[2];
            country = splitData[3];
            province = splitData[4];
            city = splitData[5];
            pageId = splitData[6];
        }
        info.put("ip",ip);
        info.put("country",country);
        info.put("province", province);
        info.put("city", city);
        info.put("url", url);
        info.put("time", time);
        info.put("pageId", pageId);
        return info;

    }

}
