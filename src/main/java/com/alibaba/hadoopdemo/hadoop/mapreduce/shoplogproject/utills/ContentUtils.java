package com.alibaba.hadoopdemo.hadoop.mapreduce.shoplogproject.utills;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentUtils {

    public static Pattern pattern = null;
    public static String getPageId(String url) {
        String pageId = "-";

        if (StringUtils.isBlank(url) || url.equals(pageId)) {
            return pageId;
        }

        Matcher matcher = Pattern.compile("topicId=[0-9]+").matcher(url);

        if (matcher.find()) {
            pageId = matcher.group().split("topicId=")[1];
        }

        return pageId;
    }
}
