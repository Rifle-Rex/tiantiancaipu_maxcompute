package reducers;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class PathStatistics extends ReducerBase {

    @Override
    public void setup(TaskContext context) throws IOException {

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Record value;
        while (values.hasNext()) {
            value = values.next();
            // TODO process value

        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}