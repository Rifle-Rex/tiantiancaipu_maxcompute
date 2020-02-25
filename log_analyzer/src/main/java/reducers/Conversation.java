package reducers;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

import java.io.IOException;
import java.util.Iterator;

public class Conversation extends ReducerBase {

    @Override
    public void setup(TaskContext context) throws IOException {

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        while (values.hasNext()) {
            values.next();
            // TODO process value
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}