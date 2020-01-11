package com.k2data.yn.test.flink;

public interface IDataAnalyseOperator<IN, OUT> {
    OUT doOperation(IN in);
}
