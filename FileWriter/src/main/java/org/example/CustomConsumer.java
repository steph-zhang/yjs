package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.io.FileWriter;
import java.io.IOException;

public class CustomConsumer {
    private static KafkaConsumer<String, String> consumer;
    public CustomConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        //每个消费者分配独立的组号
        props.put("group.id", "flink");
        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", "true");
        //设置多久一次更新被消费消息的偏移量
//        props.put("auto.commit.interval.ms", "1000");
//        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
//        props.put("session.timeout.ms", "30000");
        //自动重置offset
        //earliest 在偏移量无效的情况下 消费者将从起始位置读取分区的记录
        //latest 在偏移量无效的情况下 消费者将从最新位置读取分区的记录
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    public void consume(String output_topic, String output_file) throws Exception {
        //消费消息
        consumer.subscribe(Arrays.asList(output_topic));
        try (FileWriter writer = new FileWriter(output_file)) {
            // 持续消费数据
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(30000);

                // 处理每个记录
                records.forEach(record -> {
                    // 写入文件
                    try {
                        System.out.printf("Received record: %s%n",record.value());
                        writer.write(record.value() + "\n");
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭消费者
            consumer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String output_topic = args[0];
        String output_file = args[1];
        new CustomConsumer().consume(output_topic, output_file);
    }
}
