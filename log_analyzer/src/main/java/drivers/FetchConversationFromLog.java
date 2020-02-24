package drivers;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import mappers.Log;
import reducers.Conversation;

import java.util.LinkedHashMap;

public class FetchConversationFromLog {

    public static void main(String[] args) throws OdpsException {

        if (args[0] == null){
            return ;
        }
        String pt = args[0];
        LinkedHashMap<String, String> inputpt = new LinkedHashMap<String, String>();
        inputpt.put("pt", pt);

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString("ttcp:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString(
            "type:string,json_data:string"
        ));

        // TODO: specify input and output tables
        // 设置浏览日志表输入
        InputUtils.addTable(TableInfo.builder().tableName("raw_weblog").partSpec(inputpt).label("conversation").build(), job);
        // 设置js记录日志输入
        InputUtils.addTable(TableInfo.builder().tableName("raw_js_trace_log").partSpec(inputpt).label("conversation").build(), job);
        // 设置conv输出
        OutputUtils.addTable(TableInfo.builder().tableName("conversation_sfg").partSpec(inputpt).label("conversation").build(), job);
        // 设置标记浏览日志输出
        OutputUtils.addTable(TableInfo.builder().tableName("marked_weblog").partSpec(inputpt).label("marked_weblog").build(), job);
        // 设置标记js记录输出
        OutputUtils.addTable(TableInfo.builder().tableName("marked_js_trace_log").partSpec(inputpt).label("marked_js_trace").build(), job);
        // 设置错误统计表输出
        // OutputUtils.addTable(TableInfo.builder().tableName("").partSpec(outputpt).label("conversation").build(), job);

        // TODO: specify a mapper
        job.setMapperClass(Log.class);
        // TODO: specify a reducer
        job.setReducerClass(Conversation.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}