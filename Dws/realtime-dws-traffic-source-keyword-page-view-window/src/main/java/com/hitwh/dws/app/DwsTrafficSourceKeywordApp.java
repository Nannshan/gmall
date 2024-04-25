package com.hitwh.dws.app;

import com.hitwh.dws.function.KwSplit;
import com.hitwh.gamll.common.base.BaseSqlApp;
import com.hitwh.gamll.common.constant.Constant;
import com.hitwh.gamll.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordApp extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordApp().start(10031, 3, "dws_traffic_source_keyword_app");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务处理
        // 1. 读取主流DWD页面主题数据
        createPageInfo(tableEnv, groupId);

        // 2. 筛选出关键字keywords
        filterKeywords(tableEnv);

        // 3. 自定义UDTF函数
        tableEnv.createTemporarySystemFunction("KwSplit", KwSplit.class);

        // 4. 调用分词函数对keywords进行拆分
        KwSplitKeyWords(tableEnv);

        // 5. 对keyword进行分组开窗聚合
        //tableEnv.executeSql("select * FROM keyword_table").print();
        Table windowAggTable = getWindowAggTable(tableEnv);

        // 6. 写出到doris
        // flink需要打开检查点才能将数据写出到doris
        createDorisSink(tableEnv);

        windowAggTable.insertInto("doris_sink").execute();
    }

    private void createDorisSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE doris_sink (\n" +
                "    stt STRING,\n" +
                "    edt STRING,\n" +
                "    cur_date STRING, \n" +
                "    `keyword` STRING,\n" +
                "    keyword_count bigint\n" +
                ")" + SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
    }

    private Table getWindowAggTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n  " +
                //以便于确定是那个窗口
                "  cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING)  stt,\n" +
                "  cast(TUMBLE_END(row_time, INTERVAL '10' SECOND) as STRING )  edt,\n" +
                "  cast(CURRENT_DATE as STRING)  cur_date,\n" +
                "  keyword,\n" +
                "  count(*) keyword_count \n" +
                "FROM keyword_table\n" +
                "GROUP BY\n" +
                //十秒的滚动窗口
                "  TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "  keyword");
    }

    private void KwSplitKeyWords(StreamTableEnvironment tableEnv) {
        //两个表 join   on 后面是条件  炸裂操作，可以不写
        Table keywordTable = tableEnv.sqlQuery("SELECT keywords, keyword,`row_time` " +
                "FROM keywords_table " +
                "LEFT JOIN LATERAL TABLE(KwSplit(keywords)) ON TRUE");

        tableEnv.createTemporaryView("keyword_table",keywordTable);
    }

    private void filterKeywords(StreamTableEnvironment tableEnv) {
        Table keywordsTable = tableEnv.sqlQuery("select \n" +
                "    page['item'] keywords,\n" +
                "    `row_time` \n" +
                "from page_info\n" +
                "where page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null");

        tableEnv.createTemporaryView("keywords_table",keywordsTable);
    }

    private void createPageInfo(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table page_info(\n" +
                "    `common` map<STRING,STRING>,\n" +
                "    `page` map<STRING,STRING>,\n" +
                "    `ts` bigint,\n" +
                "    `row_time`  as TO_TIMESTAMP_LTZ(ts,3) ,\n" +
                "     WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,groupId));
    }
}
