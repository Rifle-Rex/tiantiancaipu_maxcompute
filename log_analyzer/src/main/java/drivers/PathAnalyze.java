package drivers;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import mappers.PathCategorize;
import reducers.PathStatistics;

import java.util.LinkedHashMap;

public class PathAnalyze {

    public static void main(String[] args) throws OdpsException {

        if(args[0] == null){
            return;
        }
        String pt = args[0];
        LinkedHashMap<String, String> inputPt = new LinkedHashMap<String, String>();
        inputPt.put("pt", pt);

        JobConf job = new JobConf();

        // TODO: specify map output types
        job.setMapOutputKeySchema(SchemaUtils.fromString("pathType:string,pathParameter:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("conv_id:string,ttcp:string,time:bigint,refer:string,status:bigint,bytes:bigint,method:string,ua:string,platform:string,view_time:bigint,bounced:bigint,request_type:bigint"));

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName("d_weblog").partSpec(inputPt).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("d_page_article_statistic").partSpec(inputPt).label("article").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("d_page_topic_statistic").partSpec(inputPt).label("topic").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("d_page_index_statistic").partSpec(inputPt).label("index").build(), job);

        // TODO: specify a mapper
        job.setMapperClass(PathCategorize.class);
        // TODO: specify a reducer
        job.setReducerClass(PathStatistics.class);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();

    }

}