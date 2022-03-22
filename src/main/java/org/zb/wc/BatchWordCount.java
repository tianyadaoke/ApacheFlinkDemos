package org.zb.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1.create env.
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.read file
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3. change datasource to array
        FlatMapOperator<String, Tuple2<String, Long>> operator = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. group
        UnsortedGrouping<Tuple2<String, Long>> group = operator.groupBy(0);

        // 5. aggregation
        AggregateOperator<Tuple2<String, Long>> sum = group.sum(1);

        // 6. print
        sum.print();
    }
}
