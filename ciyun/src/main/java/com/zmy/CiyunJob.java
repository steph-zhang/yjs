package com.zmy;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.zmy.pojo.PythonJob;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.List;

public class CiyunJob {
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
                            PythonJob job = new PythonJob(value.split(","));
                            String job_name = job.getJob_name();
                            String job_type = job.getJob_type();
                            JiebaSegmenter segmenter = new JiebaSegmenter();
                            List<String> strs = segmenter.sentenceProcess(job_name + job_type);
                            for(String str: strs){
                                collector.collect(Tuple2.of(str, 1));
                            }
                        }
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> {
            return value.f0;
        })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AverageAggregateFunction());

        DataStream<String> map_str = map_tuple.map(data -> data.toString());
        // ------------------------------------------------------
        // sink
        map_str.addSink(new FlinkKafkaProducer<String>("master:9092", OUTPUT_TOPIC, new SimpleStringSchema()));

        env.execute("start CiyunJob");
    }


    public static class AverageAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(Tuple2<String, Integer> value,
                                                           Tuple2<String, Integer> accumulator) {
            return new Tuple2<>(value.f0,
                    accumulator.f1 + value.f1);
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0,
                    accumulator.f1);
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a,
                                                             Tuple2<String, Integer> b) {
            return new Tuple2<>(a.f0,
                    a.f1 + b.f1);
        }

    }
}

