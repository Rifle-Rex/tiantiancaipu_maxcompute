package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.regex.Pattern;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.ReducerBase;
import com.google.gson.Gson;
import conversationObjects.Conversation;
import logObjects.JsTraceLog;
import logObjects.Log;
import logObjects.RequestUnion;
import logObjects.WebLog;

public class LogsToConversaton extends ReducerBase {

    HashMap<String, Record> outputRecords = new HashMap<>();
    Record conversationRecord;
    public static Gson gson = new Gson();

    @Override
    public void setup(TaskContext context) throws IOException {
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

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        String ttcp = key.getString("ttcp");
        ArrayList<RequestUnion> allRequestUnions = this.LogsConvToRequestUnion(values);
        // 有可能出现日志信息不全而导致返回空分组结果，这种情况不做任何处理
        if (allRequestUnions.size() == 0){
            return;
        }
        // 切分requestUnions为session对话
        ArrayList<ArrayList<RequestUnion>> allSessions = CutRequestUnionsToSessions(allRequestUnions);
        ArrayList<Conversation> allConversations = new ArrayList<>();
        for(ArrayList<RequestUnion> sessionLogs : allSessions){
            allConversations.add(AnalyzeSession(sessionLogs));
        }
        // 完成所有数据的运算与补全，开始输出
        WebLog webLog;
        for (RequestUnion ru : allRequestUnions){
            webLog = ru.webLog;
            webLog.assignmentRecord(this.outputRecords.get("WebLog"));
            context.write(this.outputRecords.get("WebLog"), "WebLog");
            if (null != ru.jsTraceLogArrayList) {
                for (JsTraceLog jsTraceLog : ru.jsTraceLogArrayList) {
                    jsTraceLog.assignmentRecord(this.outputRecords.get("JsTraceLog"));
                    context.write(this.outputRecords.get("JsTraceLog"), "JsTraceLog");
                }
            }
        }
        for (Conversation conversation: allConversations){
            conversation.ttcp = ttcp;
            conversation.assignmentRecord(this.outputRecords.get("conversation"));
            context.write(this.outputRecords.get("conversation"), "conversation");
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    static Conversation AnalyzeSession(ArrayList<RequestUnion> sessionLogs) throws IOException{
        Conversation conversation = new Conversation();
        String[] spiderUa = {"(?i).*(spider|bot).*"};
        String[] doubtUa = {".*Firefox/3\\.0b4.*", "(?!).*python.*", "(?!).*apache.*"};
        String conv_id = UUID.randomUUID().toString().replace("-", "");
        conversation.conv_id = conv_id;
        conversation.ip = sessionLogs.get(0).webLog.ip;
        conversation.ua = sessionLogs.get(0).webLog.ua;
        for (String patter : spiderUa){
            if (Pattern.matches(patter, conversation.ua)){
                conversation.conv_type = 1L;
                break;
            }
        }
        if (conversation.conv_type == 0L){
            for (String patter : doubtUa){
                if (Pattern.matches(patter, conversation.ua)){
                    conversation.conv_type = 2L;
                    break;
                }
            }
        }
        conversation.last_path = sessionLogs.get(sessionLogs.size()-1).webLog.path;
        conversation.referer = sessionLogs.get(0).webLog.refer;
        conversation.first_path = sessionLogs.get(0).webLog.path;
        conversation.js_start_time = sessionLogs.get(0).webLog.js_start_time;
        int mobilePlatformCount = 0;
        int pcPlatformCount = 0;
        int sessionLogsSize = sessionLogs.size();
        String hm_ttcp = "";
        // 从weblog或者jstracelog队列中获取hm_ttcp
        RequestUnion lastRu = sessionLogs.get(sessionLogsSize - 1);
        if (!lastRu.webLog.hm_ttcp.isEmpty()){
            hm_ttcp = lastRu.webLog.hm_ttcp;
        }
        else if (lastRu.jsTraceLogArrayList != null){
            hm_ttcp = lastRu.jsTraceLogArrayList.get(lastRu.jsTraceLogArrayList.size()-1).hm_ttcp;
        }
        Long allViewTime = 0L;
        WebLog cacheWebLog = null;
        ArrayList<JsTraceLog> cacheJsTraceLogs = null;
        for (int convIndex = 0; convIndex < sessionLogsSize; convIndex++){
            RequestUnion ru = sessionLogs.get(convIndex);
            cacheWebLog = ru.webLog;
            cacheJsTraceLogs = ru.jsTraceLogArrayList;
            cacheWebLog.conv_id = conv_id;

            // 统计js事件
            if (null != cacheJsTraceLogs) {
                for (JsTraceLog jsTraceLog : cacheJsTraceLogs) {
                    jsTraceLog.conv_id = conv_id;
                    if (jsTraceLog.event_id.equals("3")) {
                        conversation.lost_focus_count += 1;
                    }
                    if (jsTraceLog.hm_ttcp.isEmpty()){
                        jsTraceLog.hm_ttcp = hm_ttcp;
                    }
                }
                conversation.js_trace_count += cacheJsTraceLogs.size();
            }
            if (cacheWebLog.hm_ttcp.isEmpty()){
                cacheWebLog.hm_ttcp = hm_ttcp;
            }
            if (conversation.user_id == 0L && cacheWebLog.user_id != 0L){
                conversation.user_id = cacheWebLog.user_id;
            }
            conversation.request_count += 1;

            if (cacheWebLog.platform.equals("mobile")){
                mobilePlatformCount += 1;
            }
            else if (cacheWebLog.platform.equals("pc")){
                pcPlatformCount += 1;
            }

            conversation.weblog_count += 1L;

            if (cacheWebLog.status > 399L){
                conversation.error_count += 1;
            }
            else if (cacheWebLog.status == 200 || cacheWebLog.status == 304){
                conversation.pv += 1;
            }

            if (conversation.conv_type != 0L){
                cacheWebLog.request_type = conversation.conv_type;
            }
            // 计算浏览时长
            if (!ru.viewTimeComplete){
                // 以下一个请求的开始事件当作上一个请求的结束事件，若没有下一个请求则不做处理
                if (convIndex < (sessionLogsSize-1)){
                    cacheWebLog.end_time = sessionLogs.get(convIndex + 1).webLog.time;
                    if (null != ru.lastViewShowEventDate) {
                        cacheWebLog.view_time += (cacheWebLog.end_time * 1000 - ru.lastViewShowEventDate)/1000;
                    }
                    else {
                        cacheWebLog.view_time += cacheWebLog.end_time - cacheWebLog.start_time;
                    }
                }
            }
            allViewTime += cacheWebLog.view_time;

        }
        if (conversation.weblog_count == 1L){
            cacheWebLog.bounced = 1L;
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

    static ArrayList<ArrayList<RequestUnion>> CutRequestUnionsToSessions(ArrayList<RequestUnion> requestUnions){
        ArrayList<ArrayList<RequestUnion>> results = new ArrayList<>();
        ArrayList<RequestUnion> session = new ArrayList<>();
        Boolean preIsJump = false;
        Long convLastTime = 0L;
        Long thisTime = null;
        for (RequestUnion ru: requestUnions){
            if (convLastTime == 0L){
                convLastTime = ru.webLog.time;
            }
            else {
                thisTime = ru.webLog.time;
                if (thisTime - convLastTime <= 600 && !ru.webLog.refer.equals("-")){
                    convLastTime = thisTime;
                }
                else if (thisTime - convLastTime <= 10 && preIsJump){
                    convLastTime = thisTime;
                }
                else {
                    convLastTime = thisTime;
                    results.add(session);
                    session = new ArrayList<>();
                }
            }
            preIsJump = (ru.webLog.status == 302);
            session.add(ru);
        }
        if (session.size()>0){
            results.add(session);
        }
        return results;
    }

    static ArrayList<RequestUnion> LogsConvToRequestUnion(Iterator<Record> inputArrayList) throws IOException{
        ArrayList<WebLog> allWebLogs = new ArrayList<>();
        ArrayList<JsTraceLog> allJsTraceLogs = new ArrayList<>();
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
            if (logClass == WebLog.class) {
                allWebLogs.add((WebLog)cacheLog);
            }
            else if (logClass == JsTraceLog.class){
                allJsTraceLogs.add((JsTraceLog)cacheLog);
            }
        }
        return RequestUnion.groupLogsToRequestUnions(allWebLogs, allJsTraceLogs);
    }

}