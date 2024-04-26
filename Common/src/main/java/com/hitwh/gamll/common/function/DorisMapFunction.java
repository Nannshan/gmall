package com.hitwh.gamll.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T value) throws Exception {
        SerializeConfig config  = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(value, config);
    }
}
