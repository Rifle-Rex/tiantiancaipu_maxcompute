package mappers;

import java.io.IOException;
import java.util.HashMap;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

import customerException.IgnoreRecordException;
import utils.urlTools;

public class PathCategorize extends MapperBase {

    Record outputKeyRecord;
    Record outputRecord;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.outputKeyRecord = context.createOutputKeyRecord();
        this.outputRecord = context.createOutputRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        // 分析路径
        String path = record.getString("path");
        String pathType;
        String pathParameter;
        try {
            HashMap<String, String> pathResult = urlTools.analyzePathType(path);
            pathType = pathResult.get("type");
            pathParameter = pathResult.get("parameter");
        } catch (IgnoreRecordException e) {
            return;
        }
        this.outputKeyRecord.setString("pathType", pathType);
        this.outputRecord.setString("pathParameter", pathParameter);
        this.outputRecord.setString("conv_id", record.getString("conv_id"));
        this.outputRecord.setString("ttcp", record.getString("ttcp"));
        this.outputRecord.setString("refer", record.getString("refer"));
        this.outputRecord.setString("method", record.getString("method"));
        this.outputRecord.setString("ua", record.getString("ua"));
        this.outputRecord.setString("platform", record.getString("platform"));
        this.outputRecord.setBigint("time", record.getBigint("time"));
        this.outputRecord.setBigint("status", record.getBigint("status"));
        this.outputRecord.setBigint("bytes", record.getBigint("bytes"));
        this.outputRecord.setBigint("view_time", record.getBigint("view_time"));
        context.write(this.outputKeyRecord, this.outputRecord);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}