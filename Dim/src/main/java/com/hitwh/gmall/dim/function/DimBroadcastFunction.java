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
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    //预加载配置表信息
    @Override
    public void open(Configuration parameters) throws Exception {
        //预加载初始的维度表信息******解决主流数据过快所导致的主流数据丢失问题
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    //处理主流数据
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // 查询广播状态  判断当前的数据对应的表格是否存在于状态里面
        String tableName = value.getString("table");

        TableProcessDim tableProcessDim = tableProcessState.get(tableName);

        // 如果是数据到的太早  造成状态为空
        if (tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            // 状态不为空  说明当前一行数据是维度表数据
            collector.collect(Tuple2.of(value, tableProcessDim));
        }
    }

    //处理广播流数据
    @Override
    public void processBroadcastElement(TableProcessDim value, Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //读取广播状态
        BroadcastState<String, TableProcessDim> tableBroadcastState = context.getBroadcastState(broadcastState);
        //将配置表信息写入广播状态
        String op = value.getOp();
        if ("d".equals(op)) {
            tableBroadcastState.remove(value.getSourceTable());
            //同步删除hashMap中的对应数据
            hashMap.remove(value.getSourceTable());
        } else {
            //覆盖写入
            tableBroadcastState.put(value.getSourceTable(), value);
        }

    }
}
