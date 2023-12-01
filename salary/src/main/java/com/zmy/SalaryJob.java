package com.zmy;

import com.zmy.pojo.PythonJob;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class SalaryJob {
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
        DataStream<Tuple3<String, Double, Double>> map_tuple = stream.map(value -> {
                            PythonJob job = new PythonJob(value.split(","));
                            return new Tuple3<>(job.getArea(), job.get_salary_tuple().f0, job.get_salary_tuple().f1);
                        }
                ).
                returns(TypeInformation.of(new TypeHint<Tuple3<String, Double, Double>>() {}))
                .keyBy((KeySelector<Tuple3<String, Double, Double>, String>) value -> {
                    return value.f0;
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AverageAggregateFunction());



        DataStream<String> map_str = map_tuple.map(data -> data.toString());
        // ------------------------------------------------------
        // sink
        map_str.addSink(new FlinkKafkaProducer<String>("master:9092", OUTPUT_TOPIC, new SimpleStringSchema()));

        env.execute("start SalaryJob");
    }

    public static class AverageAggregateFunction implements AggregateFunction<Tuple3<String, Double, Double>, Tuple4<String, Double, Double, Integer>, Tuple3<String, Double, Double>> {

        @Override
        public Tuple4<String, Double, Double, Integer> createAccumulator() {
            return new Tuple4<>("", 0.0, 0.0, 0);
        }

        @Override
        public Tuple4<String, Double, Double, Integer> add(Tuple3<String, Double, Double> value,
                                                  Tuple4<String, Double, Double, Integer> accumulator) {
            return new Tuple4<>(value.f0,
                    accumulator.f1 + value.f1,
                    accumulator.f2 + value.f2,
                    accumulator.f3 + 1);
        }

        @Override
        public Tuple3<String, Double, Double> getResult(Tuple4<String, Double, Double, Integer> accumulator) {
            long count = accumulator.f3 != 0 ? accumulator.f3 : 1;
            return new Tuple3<>(accumulator.f0,
                    accumulator.f1 / count,
                    accumulator.f2 / count);
        }

        @Override
        public Tuple4<String, Double, Double, Integer> merge(Tuple4<String, Double, Double, Integer> a,
                                                    Tuple4<String, Double, Double, Integer> b) {
            return new Tuple4<>(a.f0,
                    a.f1 + b.f1,
                    a.f2 + b.f2,
                    a.f3 + b.f3);
        }

    }
}

