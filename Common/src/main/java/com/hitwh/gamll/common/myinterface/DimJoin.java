package com.hitwh.gamll.common.myinterface;

import com.alibaba.fastjson.JSONObject;

public interface DimJoin<T> {
    public  String getId( T input);
    public  String getTableName();
    public  void join(T input, JSONObject dim);
}
