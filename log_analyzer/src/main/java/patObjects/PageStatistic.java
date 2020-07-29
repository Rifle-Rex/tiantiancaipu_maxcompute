package patObjects;

import com.aliyun.odps.data.Record;

public class PageStatistic {
    public Long spider_crawl_count = 0L;
    public Long spider_error_crawl_count = 0L;
    public Long baidu_spider_crawl_count = 0L;
    public Long baidu_spider_error_crawl_count = 0L;
    public Long google_spider_crawl_count = 0L;
    public Long google_spider_error_crawl_count = 0L;
    public Long shenma_spider_crawl_count = 0L;
    public Long shenma_spider_error_crawl_count = 0L;
    public Long so_spider_crawl_count = 0L;
    public Long so_spider_error_crawl_count = 0L;
    public Long sogou_spider_crawl_count = 0L;
    public Long sogou_spider_error_crawl_count = 0L;
    public Long other_spider_crawl_count = 0L;
    public Long other_spider_error_crawl_count = 0L;
    public Long pv = 0L;
    public Long click_from_baidu = 0L;
    public Long click_from_google = 0L;
    public Long click_from_shenma = 0L;
    public Long click_from_so = 0L;
    public Long click_from_sogou = 0L;
    public Long click_from_other = 0L;
    public Long click_from_nowhere = 0L;
    public Long click_from_self = 0L;
    public Long error = 0L;
    public Long view_time = 0L;
    public Long bounced_count = 0L;
    public Long uv = 0L;
    public String platform = "";

    public void statistic(Record record){
        // 判定是不是蜘蛛
        Long status = record.getBigint("status");
        Long bounced = record.getBigint("bounced");
        String referer;
        String ua = record.getString("ua").toLowerCase();
        if (bounced == 1L){
            this. bounced_count++;
        }
        this.view_time += record.getBigint("view_time");
        if (record.getBigint("request_type") == 1L){
            if (status>399L){
                this.error++;
                this.spider_error_crawl_count++;
                if (ua.contains("baidu")){
                    this.baidu_spider_error_crawl_count++;
                }
                else if (ua.contains("google")){
                    this.google_spider_error_crawl_count++;
                }
                else if (ua.contains("yisouspider")){
                    this.shenma_spider_error_crawl_count++;
                }
                else if (ua.contains("360spider")){
                    this.so_spider_error_crawl_count++;
                }
                else if (ua.contains("sogou")){
                    this.sogou_spider_error_crawl_count++;
                }
                else {
                    this.other_spider_error_crawl_count++;
                }
            }
            else {
                this.pv++;
                this.spider_crawl_count++;
                if (ua.contains("baidu")){
                    this.baidu_spider_crawl_count++;
                }
                else if (ua.contains("google")){
                    this.google_spider_crawl_count++;
                }
                else if (ua.contains("yisouspider")){
                    this.shenma_spider_crawl_count++;
                }
                else if (ua.contains("360spider")){
                    this.so_spider_crawl_count++;
                }
                else if (ua.contains("sogou")){
                    this.sogou_spider_crawl_count++;
                }
                else {
                    this.other_spider_crawl_count++;
                }
            }
        }
        else {
            if (status>399L){
                this.error++;
            }
            else {
                this.pv++;
                this.view_time += record.getBigint("view_time");
                this.bounced_count += record.getBigint("bounced");
                referer = record.getString("refer");
                if (referer.equals("-")){
                    this.click_from_nowhere++;
                }
                else if (referer.contains("baidu")){
                    this.click_from_baidu++;
                }
                else if (referer.contains("google")){
                    this.click_from_google++;
                }
                else if (referer.contains("so.com")){
                    this.click_from_so++;
                }
                else if (referer.contains("sogou")){
                    this.click_from_sogou++;
                }
                else if (referer.contains("tianitancaipu")){
                    this.click_from_self++;
                }
                else if (referer.contains("sm.cn")){
                    this.click_from_shenma++;
                }
                else {
                    this.click_from_other++;
                }
            }
        }
    }
}
