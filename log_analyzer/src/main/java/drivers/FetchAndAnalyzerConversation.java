package drivers;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import mappers.ClearAndSortWebLog;
import reducers.ConversationDivision;

import java.util.LinkedHashMap;

public class FetchAndAnalyzerConversation {

    public static void main(String[] args) throws OdpsException {

        if(args[0] == null){
            return;
        }
        String pt = args[0];
        LinkedHashMap<String, String> inputPt = new LinkedHashMap<String, String>();
        inputPt.put("pt", pt);

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString("ttcp:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("object_classname:string,json_string:string"));

        // TODO: specify input and output tables
        // 设置表数据与对象的关系
        job.setStrings("tableAndObjectRelation", "cache_weblog:logObjects.WebLog", "cache_js_trace_log:logObjects.JsTraceLog");
        InputUtils.addTable(TableInfo.builder().tableName("cache_weblog").label("logObjects.WebLog").partSpec(inputPt).build(), job);
        InputUtils.addTable(TableInfo.builder().tableName("cache_js_trace_log").label("logObjects.JsTraceLog").partSpec(inputPt).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("d_weblog").label("WebLog").partSpec(inputPt).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("d_js_trace_log").label("JsTraceLog").partSpec(inputPt).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("d_conversation").label("conversation").partSpec(inputPt).build(), job);

        // TODO: specify a mapper
        job.setMapperClass(ClearAndSortWebLog.class);
        // TODO: specify a reducer
        job.setReducerClass(ConversationDivision.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}