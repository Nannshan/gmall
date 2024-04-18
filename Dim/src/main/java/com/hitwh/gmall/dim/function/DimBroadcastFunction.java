package com.hitwh.gmall.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.hitwh.gamll.common.bean.TableProcessDim;
import com.hitwh.gamll.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    public HashMap<String, TableProcessDim> hashMap ;
    public MapStateDescriptor<String, TableProcessDim> broadcast_state;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcast_state) {
        this.broadcast_state = broadcast_state;
    }

    //预加载配置表信息
    @Override
    public void open(Configuration parameters) throws Exception {
        //预加载初始的维度表信息******解决主流数据过快所导致的主流数据丢失问题
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);

        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    //处理主流数据
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(broadcast_state);
        String tablename = jsonObject.getString("table");
        TableProcessDim dim = broadcastState.get(tablename);
        if (dim != null) {
            collector.collect(Tuple2.of(jsonObject, dim));
        }else{
            dim = hashMap.get(tablename);
        }
    }

    //处理广播流数据
    @Override
    public void processBroadcastElement(TableProcessDim value, Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //读取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(broadcast_state);
        //将配置表信息写入广播状态
        String op = value.getOp();
        if ("d".equals(op)) {
            broadcastState.remove(value.getSourceTable());
            //同步删除hashMap中的对应数据
            hashMap.remove(value.getSourceTable());
        } else {
            //覆盖写入
            broadcastState.put(value.getSourceTable(), value);
        }

    }
}
