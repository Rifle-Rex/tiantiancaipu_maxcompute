package logObjects;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.google.gson.Gson;
import customerException.IgnoreRecordException;

public class Log implements Cloneable{

    /**
     * 通过record记录创建WebLog对象
     * @param record
     * @return
     */
    static Gson gson = new Gson();
    public String conv_id;  // 区分会话用
    public String ttcp = "";
    public String hm_ttcp = "";
    public Long ttl = null;
    public Long user_id = 0L;
    public String ip = "";
    public String proxy_ip = "";
    public Long time = 0L;
    public Long row_number;
    public String refer;
    public Long status;
    public Long bytes;
    public String method;
    public String path;
    public String protocol;
    public String ua;
    public String platform;
    public Double request_time;

    public static Log createFromRecord(Class logClass ,Record record) throws IllegalAccessException, InstantiationException, IgnoreRecordException, IOException {

        Log result = (Log) logClass.newInstance();
        HashMap<String, Field> resultFields = new HashMap<>();
        for (Field field: logClass.getFields()){
            resultFields.put(field.getName(), field);
        }
        Field cacheField;
        for(Column row: record.getColumns()){
            String columnName = row.getName();
            String columnType = row.getTypeInfo().toString();
            if (!resultFields.containsKey(columnName) || columnName.equals("time")){
                continue;
            }
            cacheField = resultFields.get(columnName);
            try {
                switch (columnType) {
                    case "BIGINT":
                        cacheField.set(result, record.getBigint(columnName).longValue());
                        break;
                    case "STRING":
                        cacheField.set(result, record.getString(columnName));
                        break;
                    case "DECIMAL":
                        cacheField.set(result, record.getDecimal(columnName).doubleValue());
                        break;
                    default:
                        // System.out.println("unknow column type?");
                        break;
                }
            }
            catch(IllegalAccessException e){
                e.printStackTrace();
                continue;
            }
        }
        // 最后单独获取cookies的值，分析并重新赋值关联字段
        String cookie_string = record.getString("cookies");
        if (!cookie_string.equals("-") || !cookie_string.isEmpty()){
            String[] cookies_array = cookie_string.split(";");
            HashMap<String, String> cookies = new HashMap<>();
            for(String cookie : cookies_array){
                String[] oneCookie = cookie.split("=");
                if (oneCookie.length != 2) {
                    continue;
                }
                cookies.put(oneCookie[0], oneCookie[1]);
            }
            // 从cookies中提取_ttcp、_hm_ttcp、user_id
            if (cookies.containsKey("_ttcp")){
                result.ttcp = cookies.get("_ttcp");
            }
            if (result.ttcp.isEmpty() || result.ttcp == null){
                throw new IgnoreRecordException("record not contain field 'ttcp'");
            }
            if (cookies.containsKey("_hm_ttcp")){
                result.hm_ttcp = cookies.get("_hm_ttcp");
            }
            if (cookies.containsKey("user_id")){
                try {
                    result.user_id = Long.parseLong(cookies.get("user_id"));
                }
                catch(Exception e){
                    result.user_id = 0L;
                }
            }
            if (cookies.containsKey("_ttcp_ttl")){
                try{
                    result.ttl =  Long.parseLong(cookies.get("_ttcp_ttl"));
                }
                catch(Exception e){
                    throw new IOException("_ttcp_ttl should be a number" + cookies.get("_ttcp_ttl"));
                }
            }
        }
        // 如果ip的值含有逗号，说明包含多个ip，只获取第一个值
        if (result.ip.contains(",")){
            String[] ip_list = result.ip.split(",");
            result.ip = ip_list[0];
        }

        // 同理，如果proxy_ip包含逗号，说明包含多个ip，只获取第一个值
        if (result.proxy_ip.contains(",")){
            String[] ip_list = result.proxy_ip.split(",");
            result.proxy_ip = ip_list[0];
        }

        // 时间字段需要单独转换
        String timeStr = record.getString("time");
        if (!timeStr.isEmpty()){
            result.time = WebLog.convTimeToLong(timeStr);
        }
        return result;
    }

    public void assignmentRecord(Record record) throws IOException {
        String name;
        for(Field field : this.getClass().getFields()){
            name = field.getName();
            try {
                if (null == field.get(this)){
                    record.set(name, null);
                }
                else {
                    if (field.get(this).getClass() == HashMap.class) {
                        record.set(name, Log.gson.toJson(field.get(this)));
                    }
                    else {
                        record.set(name, field.get(this));
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException("unknow type of data. class:" + field.getType().toString());
            }
        }
    }

    public static Long convTimeToLong(String time){
        HashMap<String, String> monthAbbrToNum = new HashMap<String, String>(){{
            this.put("Jan","01");
            this.put("Feb","02");
            this.put("Mar","03");
            this.put("Apr","04");
            this.put("May","05");
            this.put("Jun","06");
            this.put("Jul","07");
            this.put("Aug","08");
            this.put("Sep","09");
            this.put("Oct","10");
            this.put("Nov","11");
            this.put("Dec","12");
        }};
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss Z");
        Long returnTime = 0L;
        try {
            // 因未知原因，simpleDataFormat无法分析出英文缩写的月份，文档说可以，但是调了一下午都不成功，暂时人工转成数字
            for(String monthAbbr : monthAbbrToNum.keySet()){
                if (time.contains(monthAbbr)){
                    time = time.replace(monthAbbr, monthAbbrToNum.get(monthAbbr));
                    break;
                }
            }
            Date date = simpleDateFormat.parse(time);
            returnTime = date.getTime()/1000;
        }
        catch(ParseException e){
            // TODO:待会再想怎么处理时间格式化错误
            System.out.println("can't format date.");
            e.printStackTrace();
        }
        return returnTime;
    }

    public void debug() throws IOException {
        String name;
        for(Field field : this.getClass().getFields()){
            name = field.getName();
            try {
                System.out.println(name + ":" + field.get(this).toString());
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException("unknow type of data. class:" + field.getType().toString());
            }
        }
    }
}
