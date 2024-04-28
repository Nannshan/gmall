package com.hitwh.gamll.common.function;

import com.alibaba.fastjson.JSONObject;
import com.hitwh.gamll.common.constant.Constant;
import com.hitwh.gamll.common.myinterface.DimJoin;
import com.hitwh.gamll.common.util.HBaseUtil;
import com.hitwh.gamll.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoin<T> {
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;
    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeAsyncConnection(hBaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        String tableName = getTableName();
        String rowKey = getId(input);
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);

        //首先从redis中读取数据
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                //第一次异步访问拿到数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                }catch (Exception e){
                    e.printStackTrace();
                }
                return dimInfo;
            }
            //根据前面的信息决定是否读HBase
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = null;
                //旁路缓存
                if(dimInfo == null || dimInfo.length() == 0){
                    //访问HBase
                    try{
                        dimJsonObj = HBaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                        //包存到redis
                        redisAsyncConnection.async().setex(redisKey, 24 * 60 * 60, dimJsonObj.toJSONString());
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }else{
                    // redis中存在缓存数据
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                //合并信息
                if(jsonObject == null){
                    // 无法关联到维度信息
                    System.out.println("无法关联当前的维度信息" + tableName  + ":" + rowKey);
                }else{
                    join(input, jsonObject);
                }
                //返回结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

}

