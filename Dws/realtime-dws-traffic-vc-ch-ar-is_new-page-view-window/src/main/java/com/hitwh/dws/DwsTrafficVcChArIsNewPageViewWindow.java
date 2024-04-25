package com.hitwh.dws;

import com.hitwh.gamll.common.base.BaseApp;
import com.hitwh.gamll.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10032,3,"dws_traffic_vc_ch_ar_is_new_page_view_window_app", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

    }
}
