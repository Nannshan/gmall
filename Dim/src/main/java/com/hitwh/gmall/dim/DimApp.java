package com.hitwh.gmall.dim;

import com.hitwh.gamll.common.base.BaseApp;
import com.hitwh.gamll.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,3, "dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

    }
}
