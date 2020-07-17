package udf;

import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.udf.UDF;

public class RowSequence extends UDF {
    private LongWritable result = new LongWritable();
    public RowSequence () {
        result.set(0);
    }
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Long evaluate(Long s) {
        result.set(result.get() + 1);
        return result.get();
    }
    public Long evaluate(String s) {
        result.set(result.get() + 1);
        return result.get();
    }
    public Long evaluate() {
        result.set(result.get() + 1);
        return result.get();
    }
}