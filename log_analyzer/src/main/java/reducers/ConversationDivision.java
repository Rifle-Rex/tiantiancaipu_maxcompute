package reducers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.ReducerBase;
import com.google.gson.Gson;
import conversationObjects.Conversation;
import logObjects.*;
import java.net.URLDecoder;

public class ConversationDivision extends ReducerBase {

    HashMap<String, Record> outputRecords = new HashMap<>();
    Record conversationRecord;
    public static Gson gson = new Gson();

    @Override
    public void setup(TaskContext context) throws IOException {
        // return ;
        // 检查输出表信息，至少保证有conversation表输出设置
        TableInfo[] tableInfos = context.getOutputTableInfo();
        if (null == tableInfos){
            throw new IOException("output table info had't set.");
        }
        Boolean hasConv = false;
        String tableLabel;
        for(TableInfo tableInfo : tableInfos){
            tableLabel = tableInfo.getLabel();
            if (tableLabel.isEmpty() || tableLabel == null){
                throw new IOException("all output table should had set 'label' attr.");
            }
            if (tableLabel.equals("conversation")){
                hasConv = true;
            }
            this.outputRecords.put(tableLabel, context.createOutputRecord(tableLabel));
        }
        if (!hasConv){
            throw new IOException("at least contains a table which label is 'conversation'.");
        }
        // debug
        conversationRecord = context.createOutputRecord("conversation");
    }

    public void debug(ArrayList<Log> conversation){
        for(Log log : conversation){
            if (log.getClass() == logObjects.WebLog.class) {
                System.out.print("weblog:"+((WebLog)log).path+";");
            }
            else{
                JsTraceLog clog = (JsTraceLog)log;
                System.out.print("jslog:event_id/"+clog.event_id+",path/"+clog.path+";");
            }
        }
        System.out.println("\n");
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        String ttcp = key.getString("ttcp");
        ArrayList<Log> records = this.SessionSort(values);
        /*
        if (records.size()>1) {
            System.out.println("start----------------");
            for (Log record : records) {
                System.out.println(record.getClass().getName() + ":" + record.ttcp + "," + record.time.toString() + ","
                        + record.row_number.toString() + "," + record.path + "," + record.ua);
            }
            System.out.println("end----------------");
        }
         */

        ArrayList<ArrayList<Log>> conversationsLogs = ConversationDivision.CutSessionIntoConversation(records);
        /*
        for (ArrayList<Log> conversationLogs : conversationsLogs) {
            System.out.println("start--------------------------------------");
            this.debug(conversationLogs);
             ConversationDivision.AnalyzeConversation(conversationLogs);
            this.debug(conversationLogs);
            System.out.println("end--------------------------------------");
        }

        return;
         */
        ArrayList<Conversation> allConversations = new ArrayList<>();
        String className = "";
        for (ArrayList<Log> conversationLogs : conversationsLogs) {
            allConversations.add(ConversationDivision.AnalyzeConversation(conversationLogs));
            for(Log log : conversationLogs){
                className = log.getClass().getName();
                if (className.contains("WebLog")){
                    className = "WebLog";
                }
                else if (className.contains("JsTraceLog")){
                    className = "JsTraceLog";
                }
                else {
                    throw new IOException("unkonw className." + className);
                }
                log.assignmentRecord(this.outputRecords.get(className));
                context.write(this.outputRecords.get(className), className);
            }
        }
        for (Conversation conversation:allConversations){

            conversation.assignmentRecord(this.outputRecords.get("conversation"));
            context.write(this.outputRecords.get("conversation"), "conversation");

            /*
            conversation.assignmentRecord(this.conversationRecord);
            // conversation.debug();
            context.write(this.conversationRecord,"conversation");
             */
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    // log日志排序，key为排序列，只能是数字型的字符串才能排序；key:log_time
    static ArrayList<Log> SessionSort(Iterator<Record> inputArrayList) throws IOException{
        ArrayList<Log> result = new ArrayList<>();
        String logClassName;
        String jsonStr;
        Log cacheLog;
        Class logClass;
        while(inputArrayList.hasNext()) {
            Record val = inputArrayList.next();
            logClassName = val.getString("object_classname");
            jsonStr = val.getString("json_string");
            try {
                logClass = Class.forName(logClassName);
                if (!logObjects.Log.class.isAssignableFrom(logClass)){
                    throw new IOException("object_classname '"+logClassName+"' is not extends from logObjects.Log");
                }
                cacheLog = (Log) ConversationDivision.gson.fromJson(jsonStr, logClass);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new IOException("unknow object '"+logClassName+"'");
            }
            result.add(cacheLog);
        }
        result.sort(new Comparator<Log>() {
            @Override
            public int compare(Log o1, Log o2) {
                // 根据时间排序
                if (o1.time == o2.time){
                    if (o1.getClass() == logObjects.WebLog.class){
                        if (o2.getClass() == logObjects.JsTraceLog.class){
                            return 1;
                        }
                    }
                    else{
                        if (o1.row_number >= o2.row_number){
                            return 1;
                        }
                        else {
                            return -1;
                        }
                    }
                }
                return (int)(o1.time-o2.time);
            }
        });
        return result;
    }

    // 切分会话(可考虑在切分回话的时候就进行会话分析，这样会更快，暂时为了便于理解,分为"切分会话"与"会话分析")
    // ps: 前一会话状态为302且会话时间差在10秒以内时，当前refer为"-"也不切分会话
    static ArrayList<ArrayList<Log>> CutSessionIntoConversation(ArrayList<Log> records){
        Long convlasttime = 0L;
        ArrayList<ArrayList<Log>> convresult = new ArrayList<>();
        ArrayList<Log> convcache = new ArrayList<>();
        Boolean preIs302 = false;
        for(Log thisrow: records){
            if(convlasttime == 0){
                convlasttime = thisrow.time;
            }
            else {
                Long thistime = thisrow.time;
                //两次log_time差值在30min内 and refer不为'-'时，不切分
                if(thistime - convlasttime <= 1800 && !thisrow.refer.equals("-")){
                    convlasttime = thistime;
                }
                // 分析前一个请求的时间差是否小于10秒与前一个会话是否为302状态
                else if(thistime - convlasttime <= 10 && preIs302){
                    convlasttime = thistime;
                }
                else{
                    convlasttime = thistime;
                    convresult.add(convcache);
                    convcache = new ArrayList<>();
                }
            }
            preIs302 = thisrow.status.toString().equals("302");
            convcache.add(thisrow);
        }
        if(convcache.size() > 0){
            convresult.add(convcache);
        }
        return convresult;
    }

    // 近一步分割页面和js操作


    /**
     * 会话分析(会话一个一个地分析), 此方法会根据分析结果自动往输入的conversation中补全数据
     * @param conversationLogs
     * @return
     */
    static Conversation AnalyzeConversation(ArrayList<Log> conversationLogs) throws IOException {
        String[] spider_ua = {"(?i).*(spider|bot).*"};
        String[] doubt_ua = {".*Firefox/3\\.0b4.*", "(?!).*python.*", "(?!).*apache.*"};
        Conversation conversation = new Conversation();

        // 分析用暂时变量
        Log rowLog;
        ArrayList<Log> cacheLogs = new ArrayList<>(); // 用于暂时保存分组的log
        String cachePath = ""; // 用于保存当前分组的路径信息
        Boolean cacheHasWebLog = false; // 记录分组中是否含有weblog类型记录；
        Boolean cacheHasJsOpenLog = false; // 记录分组中是否包含事件为"打开页面"的js日志
        JsTraceLog cacheJsLog; // 用于暂时缓存Log转换成JsTraceLog的结果
        Iterator convIter = conversationLogs.iterator();
        ArrayList<HashMap<String, Object>> allPathAnalyzeResults = new ArrayList<>();
        ArrayList<Log> allLogs = new ArrayList<>(); // 因分析有可能会新增数据，所以需要重新把结果保存起来
        while (convIter.hasNext()) {
            rowLog = (Log) convIter.next();
            // 当分组为空是，直接写入，并记录信息
            if (cacheLogs.size() == 0) {
                cacheLogs.add(rowLog);
                cachePath = rowLog.path;
                cacheHasWebLog = (rowLog.getClass() == logObjects.WebLog.class);
                if (!cacheHasWebLog) {
                    cacheJsLog = (JsTraceLog) rowLog;
                    if (cacheJsLog.event_id.equals("0")) {
                        cacheHasJsOpenLog = true;
                    }
                }
            } else {
                if (!rowLog.path.equals(cachePath)) {
                    // 切分分组，并把当前log写入新分组
                    allPathAnalyzeResults.add(ConversationDivision.PathAnalyze(cacheLogs));
                    allLogs.addAll(cacheLogs);
                    cacheLogs.clear();
                    cacheLogs.add(rowLog);
                    cachePath = rowLog.path;
                    cacheHasJsOpenLog = cacheHasWebLog = false;
                } else {
                    if (rowLog.getClass() == logObjects.WebLog.class) {
                        //
                        if (cacheHasWebLog || cacheHasJsOpenLog) {
                            // 切分分组，并把当前log写入新分组
                            allPathAnalyzeResults.add(ConversationDivision.PathAnalyze(cacheLogs));
                            allLogs.addAll(cacheLogs);
                            cacheLogs.clear();
                            cacheLogs.add(rowLog);
                            cachePath = rowLog.path;
                            cacheHasJsOpenLog = cacheHasWebLog = false;
                        } else {
                            cacheHasWebLog = true;
                            cacheLogs.add(rowLog);
                        }
                    } else {
                        cacheJsLog = (JsTraceLog) rowLog;
                        switch (cacheJsLog.event_id) {
                            case "0":
                                if (cacheHasJsOpenLog) {
                                    // 切分分组，并把当前log写入新分组
                                    allPathAnalyzeResults.add(ConversationDivision.PathAnalyze(cacheLogs));
                                    allLogs.addAll(cacheLogs);
                                    cacheLogs.clear();
                                    cacheLogs.add(rowLog);
                                    cachePath = rowLog.path;
                                    cacheHasJsOpenLog = cacheHasWebLog = false;
                                } else {
                                    cacheLogs.add(rowLog);
                                    cacheHasJsOpenLog = true;
                                }
                                break;
                            case "3":
                                // 把当前记录写入分组后切分
                                cacheLogs.add(rowLog);
                                allPathAnalyzeResults.add(ConversationDivision.PathAnalyze(cacheLogs));
                                allLogs.addAll(cacheLogs);
                                cacheLogs.clear();
                                cachePath = "";
                                cacheHasJsOpenLog = cacheHasWebLog = false;
                                break;
                            default:
                                cacheLogs.add(rowLog);
                        }
                    }
                }
            }
            // 如果是最后一个请求记录，则直接切分与分析
            if (!convIter.hasNext() && cacheLogs.size() > 0) {
                allPathAnalyzeResults.add(ConversationDivision.PathAnalyze(cacheLogs));
                allLogs.addAll(cacheLogs);
            }
        }
        // 因赋值会破坏对象的引用，所以需要用对象本身的方法来修改。
        conversationLogs.clear();
        conversationLogs.addAll(allLogs);
        // 遍历所有路径分析结果，合并成会话里面部分字段的数据
        for (HashMap<String, Object> pathAnalyzeResult : allPathAnalyzeResults) {
            for (String key : pathAnalyzeResult.keySet()) {
                Long value = (Long) pathAnalyzeResult.get(key);
                switch (key) {
                    case "startTime":
                        if (conversation.start_time > value) {
                            conversation.start_time = value;
                        }
                        break;
                    case "endTime":
                        if (conversation.end_time < value) {
                            conversation.end_time = value;
                        }
                        break;
                    case "jsStartTime":
                        if (conversation.js_start_time > value) {
                            conversation.js_start_time = value;
                        }
                        break;
                    case "lostFocusCount":
                        conversation.lost_focus_count += value;
                        break;
                    case "jsTraceCount":
                        conversation.js_trace_count += value;
                        break;
                    default:
                        break;
                }
            }
        }
        String conv_id = UUID.randomUUID().toString().replace("-", ""); // 生成请求id
        conversation.conv_id = conv_id;
        conversation.ip = conversationLogs.get(0).ip;
        conversation.ua = conversationLogs.get(0).ua;
        for(String patter : spider_ua){
            if(Pattern.matches(patter, conversation.ua)){
                conversation.conv_type = 1L;
                break;
            }
        }
        if (conversation.conv_type == 0L){
            for(String patter : doubt_ua){
                if(Pattern.matches(patter, conversation.ua)){
                    conversation.conv_type = 2L;
                    break;
                }
            }
        }
        conversation.last_path = conversationLogs.get(conversationLogs.size()-1).path;
        int mobilePlatformCount = 0;
        int pcPlatformCount = 0;
        Long allViewTime = 0L;
        WebLog cacheWebLog;
        Log log;
        Long cacheEndTime = 0L;
        int conversationLogsSzie = conversationLogs.size();
        // 反向遍历修复完之后的日志数组，提取会话需要的字段信息，并且数据缺失的请求通过上下文补充(例如结束时间)
        for (int convIndex = conversationLogsSzie - 1; convIndex >= 0 ; convIndex--) {
            log = conversationLogs.get(convIndex);
            log.conv_id = conv_id;
            if (conversation.user_id == 0L && log.user_id != 0L) {
                conversation.user_id = log.user_id;
            }
            conversation.request_count += 1;

            if (log.platform.equals("mobile")) {
                mobilePlatformCount += 1;
            } else if (log.platform.equals("pc")) {
                pcPlatformCount += 1;
            }

            // 当前日志如果为

            if (log.getClass() == logObjects.WebLog.class) {
                conversation.weblog_count += 1;
                if (log.status > 399) {
                    conversation.error_count += 1;
                }
                else if(log.status == 200 || log.status == 304){
                    conversation.pv += 1;
                }
                if (conversation.referer.isEmpty()){
                    conversation.referer = log.refer;
                }
                if (conversation.first_path.isEmpty()){
                    conversation.first_path = log.path;
                }
                cacheWebLog = (WebLog) log;
                if (conversation.conv_type != 0L){
                    cacheWebLog.request_type = conversation.conv_type;
                }
                if (cacheEndTime == 0L){
                    cacheEndTime = cacheWebLog.start_time;
                }
                else {
                    if (cacheWebLog.start_time == cacheWebLog.end_time){
                        cacheWebLog.end_time = cacheEndTime;
                        cacheWebLog.view_time = cacheWebLog.end_time - cacheWebLog.start_time;
                    }
                    cacheEndTime = cacheWebLog.start_time;
                }
                allViewTime += cacheWebLog.view_time;
            }
        }
        if (pcPlatformCount > mobilePlatformCount) {
            conversation.platform = "pc";
        } else {
            conversation.platform = "mobile";
        }
        if (allViewTime > 0 && conversation.pv > 0){
            conversation.avg_time_on_page = (1.0 * allViewTime / conversation.pv);
        }
        return conversation;
    }


    /**
     * 路径日志融合分析
     *   用于分析会话中单次请求中的weblog记录与js事件记录，因为只分析一次请求的数据，要求输入的paths中只能有一个weblog记录，而js事件
     * 中event_id为0(0为打开页面事件)也只能有一个。当前方法暂不检验以上两个条件，输入数据的规范由上游检查。若上游输入的记录中没有weblog
     * 类型的数据，当前方法会根据其他js日志数据还原出一个weblog记录，并且标记为不是源数据还原的。
     * ps: 此方法会直接修改传入的对象的值，如果有新增weblog记录会直接往paths中插入，返回的是统计conv时相关的数据
     * ps2: 我知道这种用法很不人性，理应多传入一个专门收集修改后log的队列对象，直接往那里面写的话可能会更加容易让别人理解，但是。。。。。
     * 在测试一下自己对java的理解是否正确的时候发现真的可以，还挺好玩的就不改了。
     * @param paths
     * @return
     */
    static HashMap<String, Object> PathAnalyze(ArrayList<Log> paths) throws IOException {
        HashMap<String, Object> result = new HashMap<>();
        WebLog cacheWebLog = null; // 缓存weblog的日志信息，用于添加path_id
        JsTraceLog cacheJsTraceLog; // 缓存js的日志信息，用于添加path_id
        JsTraceLog cacheOpenEventJsLog = null; // 缓存记录中为打开事件的js记录；用于weblog记录不存在的时候通过js还原weblog记录
        String pathId = UUID.randomUUID().toString().replace("-", ""); // 生成请求id
        Long startTime = 0L;
        Long jsStartTime = 0L; // js事件记录的页面打开时间
        Long endTime = 0L;
        Long lostFocusCount = 0L; // 失去焦点计数
        Long jsTraceCount = 0L; // 除去打开页面与关闭页面之外的js计数
        String scrollTrace = ""; // 页面滚动数据，根据js逻辑，只有在失去焦点和关闭页面时发送，
        for (Log log : paths){
            // startTime为0时，以此判别是否为第一条数据
            if(startTime == 0L){
                // 给时间赋值
                startTime = log.time;
                if (paths.size() > 1 && startTime < paths.get(paths.size()-1).time){
                    endTime = paths.get(paths.size()-1).time;
                }
                else {
                    endTime = startTime;
                }
            }
            if (log.getClass() == logObjects.WebLog.class){
                cacheWebLog = (WebLog) log;
                cacheWebLog.path_id = pathId;
            }
            else {
                cacheJsTraceLog = (JsTraceLog) log;
                cacheJsTraceLog.path_id = pathId;
                switch(cacheJsTraceLog.event_id){
                    case "0": // 页面打开
                        cacheOpenEventJsLog = cacheJsTraceLog;
                        jsStartTime = cacheOpenEventJsLog.time;
                        break;
                    case "1": // 关闭页面
                        endTime = cacheJsTraceLog.time;
                        scrollTrace = mergeScrollTraceData(scrollTrace, cacheJsTraceLog.data.getOrDefault("scroll_trace", ""));
                        break;
                    case "3": // 失去焦点事件
                        lostFocusCount += 1L;
                        endTime = cacheJsTraceLog.time;
                        scrollTrace = mergeScrollTraceData(scrollTrace, cacheJsTraceLog.data.getOrDefault("scroll_trace", ""));
                        break;
                    default:
                        jsTraceCount++;
                }
            }
        }
        // 如果没有weblog记录，则通过js事件记录还原
        if (null == cacheWebLog){
            try {
                if (cacheOpenEventJsLog != null) {
                    cacheWebLog = cacheOpenEventJsLog.toWebLog();
                }
                else {
                    cacheWebLog = ((JsTraceLog)(paths.get(0))).toWebLog();
                }
                cacheWebLog.is_repair = 1L;
            }
            catch (IllegalAccessException e){
                throw new IOException("error when trying to transform log from js to web.");
            }
            paths.add(cacheWebLog);
        }

        // 完善weblog记录
        if (endTime >= startTime) {
            cacheWebLog.js_start_time = jsStartTime;
            cacheWebLog.start_time = startTime;
            cacheWebLog.end_time = endTime;
            if (cacheWebLog.js_start_time > 0L) {
                cacheWebLog.view_time = endTime - jsStartTime;
            }
            else {
                cacheWebLog.view_time = endTime - startTime;
            }
        }
        else {
            System.out.println(paths.get(0).ttcp);
            for (Log log : paths){
                System.out.println(log.getClass().getName());
                System.out.println(log.path);
                if (log.getClass() == JsTraceLog.class){
                    System.out.println(((JsTraceLog)log).event_id);
                }
                System.out.println(log.time);
            }
            throw new IOException("endTime should not be bigger than startTime");
        }
        result.put("startTime", startTime);
        result.put("jsStartTime", jsStartTime);
        result.put("endTime", endTime);
        result.put("lostFocusCount", lostFocusCount);
        result.put("jsTraceCount", jsTraceCount);
        return result;
    }

    // 查找url指定参数并返回结果
    static String findUrlParameter(String url, String Parametername){
        String matchresult = "";
        Pattern pathpattern = Pattern.compile("\\?(?:.*?&)?"+Parametername+"=([^&]+)");
        Matcher pathmatch = pathpattern.matcher(url);
        // 检验refer里面有没有百度参数
        if (pathmatch.find()){
            try {
                // 关于urldecode的结果，java会对“+”号的urldecode结果替换成空格。。。。不知名原因。。貌似是安全问题
                matchresult = java.net.URLDecoder.decode(pathmatch.group(1), "UTF-8").replace(" ","+");
            }
            catch(Exception ex){
                matchresult = "";
            }
        }
        return matchresult;
    }

    static String mergeScrollTraceData(String originString, String newString){
        if (originString.equals("")){
            return newString;
        }
        else if (newString.equals("")){
            return originString;
        }
        else{
            ArrayList originList = gson.fromJson(originString, ArrayList.class);
            ArrayList newList = gson.fromJson(newString, ArrayList.class);
            originList.addAll(newList);
            return gson.toJson(originList);
        }
    }
}
