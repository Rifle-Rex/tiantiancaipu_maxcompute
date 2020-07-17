package mappers;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import customerException.IgnoreRecordException;
import logObjects.Log;

public class ClearAndSortWebLog extends MapperBase {

    private HashMap<String,String> tableAndObjectRelation = new HashMap<>();
    private Gson gson;
    private Record sessionKey;
    private Record content;
    private SimpleDateFormat simpleDateFormat;
    // 路径过滤正则
    private String[] patternString = new String[] {"\\.(css|png|jpg|js|ico)(\\?|$)", "7uxyQxeYuWx|api_sz|api_cd|dynamic",
            "(qiyu_send_call_sms|api|dynamic|interface|captcha)\\.php"};
    private ArrayList<Pattern> patterns = new ArrayList<>();

    @Override
    public void setup(TaskContext context) throws IOException {
        String[] tableAndObjectRelationStrings = context.getJobConf().getStrings("tableAndObjectRelation");
        if (tableAndObjectRelationStrings.length == 0){
            throw new IOException("job Strings 'tableAndObjectRelation' should be set in the jobconf status.");
        }
        for (String tableAndObjectRelationString : tableAndObjectRelationStrings){
            String[] oneRow = tableAndObjectRelationString.split(":");
            if (oneRow.length != 2){
                throw new IOException("structure of tableAndObjectRelation is wrong.");
            }
            this.tableAndObjectRelation.put(oneRow[0], oneRow[1]);
        }
        this.simpleDateFormat = new SimpleDateFormat("[dd/MM/yyyy:HH:mm:ss Z]");
        this.sessionKey = context.createMapOutputKeyRecord();
        this.content = context.createMapOutputValueRecord();
        for(String patternStr : this.patternString){
            this.patterns.add(Pattern.compile(patternStr));
        }
        GsonBuilder builder = new GsonBuilder();
        this.gson = builder.create();
    }

    /**
     * 整理输入数据，识别输入数据的类型，序列化输出
     * @param recordNum
     * @param record
     * @param context
     * @throws IOException
     */
    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        JsonObject result = new JsonObject();
        String tableName = context.getInputTableInfo().getTableName();
        String logClassName = this.tableAndObjectRelation.get(tableName);
        // 状态码过滤,过滤4字头错误
        Long status_code = record.getBigint("status");
        if(status_code < 500L && status_code >= 400L){
            return;
        }
        String ua = record.get("ua").toString();
        if(ua.contains("ApacheBench")){
            return;
        }
        // 方法过滤，请求方法为head的过滤
        String method = record.get("method").toString();
        if(method.equals("HEAD") || method.equals("OPTION")){
            return;
        }
        // 路径过滤
        String path = record.get("path").toString();
        boolean isMatch = false;
        for(Pattern pattern : this.patterns){
            if(pattern.matcher(path).find()){
                return;
            }
        }

        // 反射方式生成对应日志类型的对象，并且json化
        Class object_class = null;
        try {
            object_class = Class.forName(logClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IOException("unknow class 'WebLog'");
        }
        Log log = null;
        try {
            // 动态调用Log类型的createFromRecord
            Method createFromRecord = object_class.getMethod("createFromRecord", Class.class, Record.class);
            log = (Log)createFromRecord.invoke(null, object_class, record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException("Can't reflectively create an instance for some 'accessable' reason.");
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new IOException("Can't find method 'createFromRecord'");
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            // 如果方法反馈的是IgnoreRecordException，则忽略当前record值
            if (t.getClass() == IgnoreRecordException.class){
                // System.out.println(t.getMessage());
                return;
            }
            e.printStackTrace();
            throw new IOException("InvocationTargetException?");
        }
        // cookies字段中提取ttcp字段为空时，放弃记录，只有带有ttcp值的log记录能够参与运算
        if(log.ttcp.isEmpty()){
            return;
        }
        String json_string = gson.toJson(log);
        sessionKey.set(new Object[]{log.ttcp});
        content.set(new Object[]{object_class.getName(), json_string});
        context.write(sessionKey, content);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}
