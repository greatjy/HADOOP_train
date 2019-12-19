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
        if(!(log == null || log == "")){
            String[] splitData = log.split("\001");
            url = splitData[1];
            ip = splitData[13];
            IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
            if(regionInfo != null){
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }
        }
        info.put("ip",ip);
        info.put("country",country);
        info.put("province", province);
        info.put("city", city);
        info.put("url", url);

        return info;

    }

}
