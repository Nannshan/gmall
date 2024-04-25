package com.hitwh.dws.function;

import com.hitwh.gamll.common.util.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSplit  extends TableFunction<Row> {
    public void eval(String keywords) {
        List<String> stringList = IKUtil.IKSplit(keywords);
        for (String s : stringList) {
            collect(Row.of(s));
        }
    }
}
