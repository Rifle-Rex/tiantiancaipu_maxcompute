package utils;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class urlTools {

    static HashMap<String, Pattern> pathPatternAndTypeMap = new HashMap<String, Pattern>(){{
        this.put("index", Pattern.compile("^/\\??")); // 首页
        this.put("article_list", Pattern.compile("^/articles(/list-\\d+\\.html|/?(\\??|$))")); // 文章列表
        this.put("article", Pattern.compile("^/articles/(info-)?\\d+\\.html")); // 文章内容页
        this.put("recipes", Pattern.compile("^/recipes/(info-)?\\d+\\.html")); // 文章内容页
        this.put("topic", Pattern.compile("/(\\w+)/food-\\d+(-\\d+)?\\.html")); // 专题页面
        this.put("topic_list", Pattern.compile("^/\\w+/?(\\??|$)")); // 专题列表页
    }};

    public static String analyzePathType(String path){
        Pattern cachePattern;
        Matcher cacheMatcher;
        String pathType = "";
        for(String type : urlTools.pathPatternAndTypeMap.keySet()){
            cachePattern = urlTools.pathPatternAndTypeMap.get(type);
            cacheMatcher = cachePattern.matcher(path);
            if (cacheMatcher.find()){
                pathType = type;
                break;
            }
        }
        return pathType;
    }

    public static HashMap<String, String> convertHTTPQueryToHashMap(String query){
        HashMap<String, String> result = new HashMap<>();
        String[] querys = query.split("&");
        String[] oneRow;
        for(String queryStr : querys){
            oneRow = queryStr.split("=");
            if (oneRow.length == 1){
                result.put(oneRow[0], "");
            }
            else if(oneRow.length == 2){
                result.put(oneRow[0], oneRow[1]);
            }
        }
        return result;
    }
}
