package utils;

import customerException.IgnoreRecordException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URLDecoder;

public class urlTools {

    static HashMap<String, Pattern> pathPatternAndTypeMap = new HashMap<String, Pattern>(){{
        this.put("index", Pattern.compile("^/\\??")); // 首页
        this.put("article_list", Pattern.compile("^/articles(?:/list-\\d+\\.html|/?(?:\\??|$))")); // 文章列表
        this.put("article", Pattern.compile("^/articles/(?:info-)?(?<id>\\d+)\\.html")); // 文章内容页
        this.put("recipes", Pattern.compile("^/recipes/(?:info-)?(?<id>\\d+)\\.html")); // 文章内容页
        this.put("topic", Pattern.compile("/(\\w+)/food-(?<id>\\d+)(?:-\\d+)?\\.html")); // 专题页面
        this.put("topic_list", Pattern.compile("^/(?<type>\\w+)/?(\\??|$)")); // 专题列表页
    }};

    private static HashMap<String, ArrayList<String>>pathPatternGroupName = new HashMap<>();

    public static HashMap<String, String> analyzePathType(String path) throws IgnoreRecordException {
        HashMap<String, String> result = new HashMap<>();
        if (urlTools.pathPatternGroupName.isEmpty()) {
            String value;
            Pattern namePatter = Pattern.compile("\\?:<(\\w+)>");
            for(String key : urlTools.pathPatternAndTypeMap.keySet()){
                value = urlTools.pathPatternAndTypeMap.get(key).pattern();
                Matcher matcher = namePatter.matcher(value);
                ArrayList<String> nameList = new ArrayList<>();
                while(matcher.find()){
                    nameList.add(matcher.group(1));
                }
                urlTools.pathPatternGroupName.put(key, nameList);
            }
        }
        Pattern cachePattern;
        Matcher cacheMatcher;
        String pathType = "";
        ArrayList<String> pathGroups = new ArrayList<>();
        for(String type : urlTools.pathPatternAndTypeMap.keySet()){
            cachePattern = urlTools.pathPatternAndTypeMap.get(type);
            cacheMatcher = cachePattern.matcher(path);
            if (cacheMatcher.find()){
                result.put("type", type);
                if (!urlTools.pathPatternGroupName.containsKey(type)){
                    throw new IgnoreRecordException("unkonw path type");
                }
                for (String groupName: urlTools.pathPatternGroupName.get(type)){
                    pathGroups.add(groupName + "=" + cacheMatcher.group(groupName));
                }
                result.put("parameter", String.join(",", pathGroups));
                break;
            }
        }
        if (result.isEmpty()){
            throw new IgnoreRecordException("unknow path type.");
        }
        return result;
    }

    public static HashMap<String, String> convertHTTPQueryToHashMap(String query) throws IOException{
        HashMap<String, String> result = new HashMap<>();
        try {
            query = URLDecoder.decode(query, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new IOException("can't encode query string to utf-8");
        }
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
