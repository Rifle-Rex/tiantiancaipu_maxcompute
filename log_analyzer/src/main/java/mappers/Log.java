package mappers;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

import java.io.IOException;

public class Log extends MapperBase {

    public Record ttcp;
    public Record outputValue;

    @Override
    public void setup(TaskContext context) throws IOException {
        ttcp = context.createMapOutputKeyRecord();
        outputValue = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        // TODO
        // 通过platform值判定来源，不是很好的方法，需要修改
        String type;
        String platform = record.getString("platform");
        if (platform.equals("")){
            type = "js_trace";
        }
        else {

        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}