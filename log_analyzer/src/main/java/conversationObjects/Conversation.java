package conversationObjects;

import com.aliyun.odps.data.Record;

import java.io.IOException;
import java.lang.reflect.Field;

public class Conversation {
    public String conv_id;
    public Long request_count = 0L; // 请求计数
    public Long weblog_count = 0L; // 日志计数
    public Long js_trace_count = 0L; // js事件计数
    public Long error_count = 0L; // 错误请求计数，只统计weblog
    public String ip = ""; // ip，同一会话中多个ip以逗号链接
    public String ua = "";
    public String platform = ""; // 平台，一个会话中如果有多中平台日志记录，以请求数多的为准
    public Long pv = 0L; // page views
    public Long user_id=0L; // 用户id
    public Double avg_time_on_page = 0.0; // 平均页面停留时间
    public String referer = ""; // 回来来源，获取自第一个请求的referer
    public String first_path = ""; // 会话第一个路径
    public String last_path = ""; // 会话最后一个路径
    public Long lost_focus_count = 0L;
    public Long start_time = Long.MAX_VALUE; // 会话开始时间
    public Long end_time = 0L; // 会话结束时间
    public Long conv_type = 0L; // 会话类型 0:普通会话;1:蜘蛛会话;2:异常会话
    public Long js_start_time = Long.MAX_VALUE; // 会话第一个js_open的时间

    public void assignmentRecord(Record record) throws IOException {
        String name;
        for(Field field : this.getClass().getFields()){
            name = field.getName();
            try {
                record.set(name, field.get(this));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException("unknow type of data. class:" + field.getType().toString());
            }
        }
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
