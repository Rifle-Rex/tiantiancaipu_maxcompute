package logObjects;

public class WebLog extends Log {
    public Long is_repair = 0L; // 用于辨别当前weblog记录是weblog源数据还是从jstrace事件分析出来的。0:源数据, 1:js复原数据
    public Long js_start_time = 0L; // 若没有则为0
    public Long start_time = 0L; // 浏览页面开始时间
    public Long end_time; // 浏览页面结束时间，js记录时间优先，次之下一条关联日志记录，运算结束后若没有则与startTime相同
    public Long view_time = 0L; // 浏览时长，由startTime和endTime相减算出
    public String path_id = ""; // 当前请求id
    public Long request_type = 0L; // 请求类型，以区分是否是普通用户、蜘蛛、异常请求
}
