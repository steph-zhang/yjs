package com.zmy;

import com.zmy.pojo.PythonJob;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class PythonDegreeJob {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final String INPUT_TOPIC = "test";
        final String OUTPUT_TOPIC = "output";

        KafkaSource<String> kfkSource = KafkaSource.<String>builder()
                .setBootstrapServers("master:9092")
                .setGroupId("flink")
                .setTopics(INPUT_TOPIC)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> stream = env.fromSource(kfkSource, WatermarkStrategy.noWatermarks(), "kafka source");

        // transform------------------------------------------------------
        SingleOutputStreamOperator<Tuple2<String, Integer>> map_tuple = stream.flatMap((String value, Collector<Tuple2<String, Integer>> collector) -> {
                            String[] job = value.split(",");
                            collector.collect(Tuple2.of(new PythonJob(job).getDegree(), 1));
                        }
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> map_tuple = stream.flatMap((String value, Collector<Tuple2<String, Integer>> collector) -> {
//                            String[] job = value.split(",");
//                            collector.collect(Tuple2.of(new PythonJob(job).getDegree(), 1));
//                        }
//                )
//                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> {
//                    return value.f0;
//                })
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .aggregate(new PythonAreaJob.AverageAggregateFunction());

        DataStream<String> map_str = map_tuple.map(data -> data.toString());
        // ------------------------------------------------------
        // sink
        map_str.addSink(new FlinkKafkaProducer<String>("master:9092", OUTPUT_TOPIC, new SimpleStringSchema()));

        env.execute("start FlinkPythonJob");
    }


}
