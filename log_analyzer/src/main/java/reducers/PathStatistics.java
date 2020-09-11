package reducers;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import customerException.IgnoreRecordException;
import jdk.nashorn.internal.ir.annotations.Ignore;
import patObjects.PageStatistic;
import utils.urlTools;

public class PathStatistics extends ReducerBase {

    Record articleRecord;
    Record topicRecord;
    Record indexRecord;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.articleRecord = context.createOutputRecord("article");
        this.topicRecord = context.createOutputRecord("topic");
        this.indexRecord = context.createOutputRecord("index");
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        PageStatistic pcPageStatistic = new PageStatistic();
        PageStatistic mobilePageStatistic = new PageStatistic();
        PageStatistic cachePageStatistic = null;
        pcPageStatistic.platform = "pc";
        mobilePageStatistic.platform = "mobile";
        HashSet<String> ttcp_list = new HashSet<>();
        String pathType = key.getString("pathType");
        String pathParameter = key.getString("pathParameter");
        HashMap<String, String> pathParametersHashMap = urlTools.convertHTTPQueryToHashMap(pathParameter);
        String pathCategory;
        Record value;
        Record outputRecord = null;
        switch (pathType){
            case "index":
                pathCategory = "index";
                outputRecord = this.indexRecord;
                break;
            case "topic":
                pathCategory = "topic";
                outputRecord = this.topicRecord;
                outputRecord.set("type", pathParametersHashMap.get("type"));
                outputRecord.set("id", pathParametersHashMap.get("id"));
                break;
            case "articles":
            case "recipes":
            case "formula":
                pathCategory = "article";
                outputRecord = this.articleRecord;
                outputRecord.set("type", pathType);
                outputRecord.set("id", pathParametersHashMap.get("id"));
                break;
            default:
                // throw new IOException("unknow pathType");
                return;
        }

        while (values.hasNext()) {
            value = values.next();
            switch(value.getString("platform")){
                case "pc":
                    cachePageStatistic = pcPageStatistic;
                    break;
                case "mobile":
                    cachePageStatistic = mobilePageStatistic;
                    break;
                default:
                    throw new IOException("unknow platform");
            }
            cachePageStatistic.statistic(value);
            if (value.getBigint("request_type") == 0L && !ttcp_list.contains(value.getString("ttcp"))){
                ttcp_list.add(value.getString("ttcp"));
                cachePageStatistic.uv += 1;
            }
        }

        String fieldName;
        Object fieldValue;
        // 写入pc平台的数据
        for (Field field : PageStatistic.class.getFields()){
            fieldName = field.getName();
            try {
                fieldValue = field.get(pcPageStatistic);
                outputRecord.set(fieldName, fieldValue);

            } catch (IllegalAccessException e) {
                continue;
            }
        }
        context.write(outputRecord, pathCategory);
        // 写入mobile平台的数据
        for (Field field : PageStatistic.class.getFields()){
            fieldName = field.getName();
            try {
                fieldValue = field.get(mobilePageStatistic);
                outputRecord.set(fieldName, fieldValue);
            } catch (IllegalAccessException e) {
                continue;
            }
        }
        context.write(outputRecord, pathCategory);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }
}