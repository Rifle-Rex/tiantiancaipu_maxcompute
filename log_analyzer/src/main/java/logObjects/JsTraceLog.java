package logObjects;

import com.aliyun.odps.data.Record;
import customerException.IgnoreRecordException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import utils.urlTools;
public class JsTraceLog extends Log {
    public HashMap<String, String> data = null;
    public String event_id;
    public String path_id = ""; // 用于记录当前事件与请求记录的关系
    public Long js_ttl = null;
    public String js_h = "";

    public static JsTraceLog createFromRecord(Class logClass , Record record) throws IllegalAccessException, InstantiationException, IgnoreRecordException, IOException {
        JsTraceLog result = (JsTraceLog)Log.createFromRecord(logClass, record);
        String dataStr = "";
        // 处理path字段
        try {
            // 因为js_trace的请求页面路径是通过refer来记录的，本身的path除了获取参数外并没有太多意义，为了和weblog统一统计口径，从refer中提取path并且根据域名补充platform字段
            URL url;
            try {
                url = new URL(result.refer);
            }
            catch(Exception e){
                throw new IgnoreRecordException("refer is illegal");
            }
            result.path = url.getFile();
            String domain = url.getHost();
            if (domain.equals("m.tiantiancaipu.com")){
                result.platform = "mobile";
            }
            else if (domain.equals("www.tiantiancaipu.com")){
                result.platform = "pc";
            }
            else{
                throw new IgnoreRecordException("host of js_trace log is illegal. host:" + domain);
            }
            // 提取data数据，提取eventid并且转换成HashMap格式
            if (result.method.equals("POST")){
                dataStr = record.getString("post_data");
            }
            else{
                String[] oriPaths = record.getString("path").split("\\?");
                if (oriPaths.length == 2){
                    dataStr = oriPaths[1];
                }
            }
            result.data = urlTools.convertHTTPQueryToHashMap(dataStr);
            if (!result.data.containsKey("event_id")){
                throw new IgnoreRecordException("event_id should be set in a js_trace data");
            }
            result.event_id = result.data.get("event_id");
            result.data.remove("event_id");
            result.js_h = result.data.get("js_h");
            try {
                result.ttl = Long.parseLong(result.data.get("ttl"));
                result.js_ttl = Long.parseLong(result.data.get("js_ttl"));
                result.data.remove("ttl");
                result.data.remove("js_ttl");
                result.data.remove("js_h");
            }
            catch (Exception e){ ;
                // throw new IOException("ttl or js_ttl should be number.");
                throw new IgnoreRecordException("js log record without ttl or js_ttl is ignore.");

            }
            return result;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            throw new IllegalAccessException("url is illegal.");
        }
    }

    public WebLog toWebLog() throws IllegalAccessException {
        WebLog result = new WebLog();
        Field[] allLogField = this.getClass().getSuperclass().getFields();
        String name;
        Object value;
        for (Field field : allLogField){
            name = field.getName();
            try {
                if (null != WebLog.class.getField(name)) {
                    value = field.get(this);
                    field.set(result, value);
                }
            } catch (NoSuchFieldException e) {
                continue;
            }
        }
        // 如果当前对象的event_id为0，则data中有可能有refer信息，需要单独补全
        if (this.event_id.equals("0") && this.data.containsKey("referer")){
            result.refer = this.data.get("referer");
        }
        return result;
    }
}
