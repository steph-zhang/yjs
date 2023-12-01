package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Properties;

public class FileWatcher {

    public static void main(String[] args) throws IOException {
        String input_topic = args[0];
        String data_dir = args[1];
        // 设置 Kafka 生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建 Kafka 生产者
        Producer<Object, String> producer = new KafkaProducer<>(props);

        // 监视的文件夹路径
        String folderPath = data_dir;

        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            Path path = Paths.get(folderPath);

            // 监视文件夹的创建事件
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        // 获取新创建的文件的路径
                        Path newPath = ((WatchEvent<Path>) event).context();
                        Path newFilePath = path.resolve(newPath);

                        // 读取文件内容
                        List<String> fileContent = readFileContent(newFilePath);

                        // 发送到 Kafka 主题
                        for(String line: fileContent) {
                            producer.send(new ProducerRecord<>(input_topic, line));
                        }
                        System.out.println("New file added to Kafka: " + newFilePath);
                    }
                }
                key.reset();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static List<String> readFileContent(Path filePath) throws IOException {
        // 读取文件内容
        List<String> fileLines = Files.readAllLines(filePath);
        return fileLines;
    }
}


