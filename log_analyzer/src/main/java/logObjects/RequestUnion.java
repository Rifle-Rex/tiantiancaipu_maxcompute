package logObjects;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.UUID;

/**
 * 一个请求所产生的所有日志的集合，包括这次请求的网站日志，js请求日志s等
 */

public class RequestUnion{
    static Gson gson = new Gson();
    public WebLog webLog;
    public ArrayList<JsTraceLog> jsTraceLogArrayList;
    public int status = 0; // 对象状态，0为刚创建，1为已经根据当前对象中的日志记录运算和提取结果了
    public Boolean viewTimeComplete = true;
    public Long lastViewShowEventDate = null;


    public RequestUnion(WebLog webLog, ArrayList<JsTraceLog> jsTraceLogArrayList){
        this.webLog = webLog;
        this.jsTraceLogArrayList = jsTraceLogArrayList;
    }

    // 根据输入的weblog和jstracelog中的ttl、js_h、js_ttl进行分组、排序和关联，最终合成已经排序好的RequestUnion对象数组
    public static ArrayList<RequestUnion> groupLogsToRequestUnions(ArrayList<WebLog> webLogsArrayList,
                                                                   ArrayList<JsTraceLog> jsTraceLogArrayList) throws IOException{
        ArrayList<RequestUnion> result = new ArrayList<>();
        // web日志数组排序
        webLogsArrayList.sort(new Comparator<WebLog>() {
            @Override
            public int compare(WebLog o1, WebLog o2) {
                if (o1.time == o2.time){
                    if (o1.ttl == o2.ttl){
                        throw new ClassCastException("ttl should't be the same.");
                    }
                    else{
                        return (int)(o1.ttl - o2.ttl);
                    }
                }
                else{
                    return (int)(o1.time - o2.time);
                }
            }
        });
        // js日志根据js_h分组
        HashMap<String, ArrayList<JsTraceLog>> groupJsTraceLogByJshHashMap = new HashMap<>();
        for (JsTraceLog jsTraceLog : jsTraceLogArrayList){
            if (!groupJsTraceLogByJshHashMap.containsKey(jsTraceLog.js_h)){
                groupJsTraceLogByJshHashMap.put(jsTraceLog.js_h, new ArrayList<JsTraceLog>());
            }
            groupJsTraceLogByJshHashMap.get(jsTraceLog.js_h).add(jsTraceLog);
        }
        // js日志记录排序
        ArrayList<ArrayList<JsTraceLog>>allGroupJsTraceLogArrayList = new ArrayList<>();
        for (ArrayList<JsTraceLog> groupJsTraceLogArrayList : groupJsTraceLogByJshHashMap.values()){
            groupJsTraceLogArrayList.sort(new Comparator<JsTraceLog>() {
                @Override
                public int compare(JsTraceLog o1, JsTraceLog o2) {
                    if (o1.js_ttl == o2.js_ttl){
                        if (o1.time == o2.time){
                            throw new ClassCastException("ttl and time equal in the same time.");
                        }
                        else {
                            return (int) (o1.time - o2.time);
                        }
                    }
                    else{
                        return (int)(o1.js_ttl-o2.js_ttl);
                    }
                }
            });
            allGroupJsTraceLogArrayList.add(groupJsTraceLogArrayList);
        }
        groupJsTraceLogByJshHashMap = null;
        // 根据融合weblog和groupJsTraceLog
        for(WebLog weblog: webLogsArrayList){
            Boolean hadMatch = false;
            int i;
            for (i = 0; i < allGroupJsTraceLogArrayList.size(); i++){
                ArrayList<JsTraceLog> thisJsTraceLogArrayList = allGroupJsTraceLogArrayList.get(i);
                JsTraceLog firstJsTraceLog = thisJsTraceLogArrayList.get(0);
                if (weblog.ttl == firstJsTraceLog.ttl){
                    // 判定页面js事件是否使用本地页面缓存，是则跳过，因为缓存不会产生服务器日志
                    if (firstJsTraceLog.event_id.equals("0") && firstJsTraceLog.data.containsKey("useCache")
                        && firstJsTraceLog.data.get("useCache").equals("1")){
                        continue;
                    }
                    result.add(new RequestUnion(weblog, thisJsTraceLogArrayList));
                    hadMatch = true;
                    break;
                }
            }
            if (hadMatch){
                allGroupJsTraceLogArrayList.remove(i);
                i--;
            }
            else{
                result.add(new RequestUnion(weblog, null));
            }
        }
        // 检查allGroupJsTraceLogArrayList的长度是否为0，是则说明js事件和web记录都匹配上，
        // 否则有可能有浏览器缓存页面事件，需要通过js记录还原weblog
        if (allGroupJsTraceLogArrayList.size() > 0){
            for (ArrayList<JsTraceLog> oneGroupJsTraceLogArrayList : allGroupJsTraceLogArrayList){
                // 判断第一个js请求是否为打开页面事件，如果不是则放弃
                if (!oneGroupJsTraceLogArrayList.get(0).event_id.equals("0")){
                    continue;
                }
                try {
                    WebLog newWebLog = oneGroupJsTraceLogArrayList.get(0).toWebLog();
                    result.add(new RequestUnion(newWebLog, oneGroupJsTraceLogArrayList));
                }
                catch(IllegalAccessException e){
                    throw new IOException("IllegalAccessException alert.");
                }
            }
        }
        // 最后再排序一个result的结果
        result.sort(new Comparator<RequestUnion>() {
            @Override
            public int compare(RequestUnion o1, RequestUnion o2) {
                if (o1.webLog.ttl == o2.webLog.ttl){
                    if (o1.webLog.time == o2.webLog.time){
                        return 1;
                    }
                    else {
                        return (int)(o1.webLog.time - o2.webLog.time);
                    }
                }
                else {
                    return (int)(o1.webLog.ttl - o2.webLog.ttl);
                }
            }
        });
        for (int i = 0; i < result.size(); i++){
            result.get(i).analyzeRequest();
        }
        return result;
    }

    private boolean analyzeRequest() throws IOException{
        // 生成path_id，关联weblog和jstracelog记录
        String path_id = UUID.randomUUID().toString().replace("-", "");
        this.webLog.path_id = path_id;
        Long cacheShowPageDate = null;
        Long cacheHidePageDate = null;
        Long viewTime = 0L;
        Boolean pageHidden = true;
        Boolean lastEventPageHidden = null;
        // 合并js事件中的浏览位置数据，并提取出来
        // 计算页面停留时间.逻辑如下:
        //   1.如果没有结束时间则留null，留给session计算时通过上下文补全
        //   2.单独计算每个与页面状态有关的事件时间，最后相加得出(将事件分为两类，页面可视与页面不可视，只统计状态变化的最小页面可视时间与最大页面不可视时间的时间差)
        //   3.如果事件缺失无法获得完整的页面展示与不展示时间，则记录最后一次可视事件的事件，并且记录页面停留时间统计状态为不完全，留给session计算时补全
        if (this.jsTraceLogArrayList == null){
            this.viewTimeComplete = false;
        }
        else {
            for (JsTraceLog jsTraceLog : this.jsTraceLogArrayList) {
                jsTraceLog.path_id = path_id;
                Long jsDate = Long.parseLong(jsTraceLog.data.get("date"));
                // 特殊事件处理
                if (jsTraceLog.event_id.matches("^0|1|3|4|5$")) {
                    switch (jsTraceLog.event_id) {
                        // 打开页面事件
                        case "0":
                            this.webLog.js_start_time = jsDate;
                            // 页面得到焦距
                        case "4":
                            pageHidden = false;
                            break;
                        // 页面失去焦距
                        case "3":
                            // 关闭页面事件
                        case "1":
                            pageHidden = true;
                            break;
                        case "5":
                            if (!jsTraceLog.data.containsKey("hidden")){
                                // 不知名原因存在data为null的情况。
                                continue;
                            }
                            if (jsTraceLog.data.get("hidden").equals("true")) {
                                pageHidden = true;
                            } else if (jsTraceLog.data.get("hidden").equals("false")) {
                                pageHidden = false;
                            } else {
                                System.out.println(jsTraceLog.data);
                                throw new IOException("parameter 'hidden' is missing when event_id = 5");
                            }
                            break;
                    }
                    // 第一次出现特殊事件时，需要特殊处理
                    if (lastEventPageHidden == null) {
                        lastEventPageHidden = false;
                        if (!pageHidden) {
                            cacheShowPageDate = jsDate;
                        } else {
                            cacheShowPageDate = this.webLog.time * 1000;
                            cacheHidePageDate = jsDate;
                            lastEventPageHidden = true;
                            viewTime += cacheHidePageDate - cacheShowPageDate;
                        }
                        continue;
                    } else {
                        if (pageHidden) {
                            cacheHidePageDate = jsDate;
                        } else {
                            cacheShowPageDate = jsDate;
                        }
                        if (pageHidden != lastEventPageHidden && pageHidden) {
                            if (cacheHidePageDate < cacheShowPageDate) {
                                throw new IOException("js view start date should be smaller than view end date.");
                            }
                            viewTime += cacheHidePageDate - cacheShowPageDate;
                        }
                    }
                }
            }
            this.webLog.view_time = viewTime;
            if (!pageHidden) {
                this.viewTimeComplete = false;
                this.lastViewShowEventDate = cacheShowPageDate;
            } else {
                this.webLog.end_time = cacheHidePageDate/1000;
            }
        }
        this.status = 1;
        return true;
    }
}
