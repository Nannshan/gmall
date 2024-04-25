package com.hitwh.gmall.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.hitwh.gamll.common.base.BaseApp;
import com.hitwh.gamll.common.bean.TableProcessDwd;
import com.hitwh.gamll.common.constant.Constant;
import com.hitwh.gamll.common.util.FlinkSinkUtil;
import com.hitwh.gamll.common.util.FlinkSourceUtil;

import com.hitwh.gamll.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DwdBaseDbApp extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDbApp().start(10028, 3, "dwd_base_db_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1.读取topic_db数据
        //stream.print();

        // 2. 清洗过滤和转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = flatMapToJsonObj(stream);

        // 3. 读取配置表数据 使用flinkCDC读取
        DataStreamSource<String> dwdConfigStream = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);

        // 4.数据类型转换，根据 op 的不同，数据在json中的不同位置
        SingleOutputStreamOperator<TableProcessDwd> pocessDwdStream = flatMapToProcessDwd(dwdConfigStream);

        // 5.进行广播
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = pocessDwdStream.broadcast(mapStateDescriptor);

        // 6. 连接主流和广播流 对主流数据进行判断是否需要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = processBaseDb(jsonObjStream, mapStateDescriptor, broadcastStream);

        // 筛选最后需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataStream = filterColumns(processStream);

        // 将多个表格的数据写出到同一个主题 在 KafkaRecordSerializationSchema<>() 方法中 重写 ProducerRecord 方法
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopicName());
    }

    private SingleOutputStreamOperator<JSONObject> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream) {
        return processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObj.getJSONObject("data");
                List<String> columns = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key));
                data.put("sink_table",processDwd.getSinkTable());
                return data;
            }
        });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processBaseDb(SingleOutputStreamOperator<JSONObject> jsonObjStream, MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor, BroadcastStream<TableProcessDwd> broadcastStream) {
        return jsonObjStream.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
            HashMap<String, TableProcessDwd> hashMap = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                Connection connection = JdbcUtil.getMysqlConnection();
                List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(connection, "select * from gmall_config.table_process_dwd", TableProcessDwd.class, true);
                for (TableProcessDwd t : tableProcessDwds) {
                    hashMap.put(t.getSourceTable() + ":" + t.getSourceType(),t);
                }
            }

            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                // 调用广播状态判断当前时候是否需要保留
                String table = value.getString("table");
                String type = value.getString("type");
                String key = table + ":" + type;
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDwd processDwd = broadcastState.get(key);
                if(broadcastState == null){
                    processDwd = hashMap.get(key);
                }
                if (processDwd != null) {
                    out.collect(Tuple2.of(value, processDwd));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDwd value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String op = value.getOp();
                String key = value.getSourceTable()+ ":" +value.getSourceType();
                if("d".equals(op)){
                    broadcastState.remove(key);
                    hashMap.remove(key);
                }else{
                    broadcastState.put(key, value);
                }

            }
        }).setParallelism(1);
    }

    private SingleOutputStreamOperator<TableProcessDwd>  flatMapToProcessDwd(DataStreamSource<String> tableProcessDwd) {
        return tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String value, Collector<TableProcessDwd> collector) throws Exception {
                try{
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDwd processDwd;
                    if ("d".equals(op)) {
                        processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    collector.collect(processDwd);
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("捕获脏数据" + value);
                }

            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> flatMapToJsonObj(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("清洗掉脏数据" + value);
                }
            }
        });
    }
}
