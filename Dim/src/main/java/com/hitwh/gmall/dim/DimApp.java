package com.hitwh.gmall.dim;

import com.alibaba.fastjson.JSONObject;
import com.hitwh.gamll.common.base.BaseApp;
import com.hitwh.gamll.common.bean.TableProcessDim;
import com.hitwh.gamll.common.constant.Constant;
import com.hitwh.gamll.common.util.FlinkSourceUtil;
import com.hitwh.gamll.common.util.HBaseUtil;
import com.hitwh.gmall.dim.function.DimBroadcastFunction;
import com.hitwh.gmall.dim.function.DimHBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,3, "dim_app", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //stream.print();
        //1.数据清洗
        SingleOutputStreamOperator<JSONObject> jsonObjStream= etl(stream);
        //jsonObjStream.print();
        
        //2.使用FlinkCDC读取配置表，创建广播流
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        DataStreamSource<String> mysqlStream  = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

        //3.在Hbase创建维度表
        SingleOutputStreamOperator<TableProcessDim> hbaseStream  = createHbaseTable(mysqlStream).setParallelism(1);

        //4.将广播流和主流join
        //4.1做成广播流 -- key判断是否是维度表，value做补充信息写到hbase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = hbaseStream.broadcast(broadcastState);
        //4.2连接广播流和主流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = jsonObjStream.connect(broadcastStateStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectedStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
        //dimStream.print();

        //5.筛选需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> data = filterColumn(dimStream);

        //6.写入Hbase
        data.print();
        data.addSink(new DimHBaseSinkFunction());
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>>  filterColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> data = dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDim tpDim = value.f1;

                String sinkColumns = tpDim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObject.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
        return data;
    }


    public SingleOutputStreamOperator<TableProcessDim> createHbaseTable(DataStreamSource<String> mysqlStream) {
        SingleOutputStreamOperator<TableProcessDim> hbaseStream = mysqlStream.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection con;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                con = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                //关闭连接
                HBaseUtil.closeConnection(con);
            }

            //使用读取的配置表数据，创建表格
            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        deleteTable(dim);
                    } else if ("u".equals(op)) {
                        //****修改则先删除after，再重新创建****
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    } else {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    collector.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] split = sinkFamily.split(",");

                try {
                    HBaseUtil.createTable(con, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HBaseUtil.dropTable(con, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        return hbaseStream;
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) &&
                            !"bootstrap-complete".equals(type) && !"bootstrap-start".equals(type)
                            && data.size() != 0 ) {
                        collector.collect(jsonObject);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        return jsonStream;
    }
}
