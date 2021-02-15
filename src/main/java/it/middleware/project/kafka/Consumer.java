package it.middleware.project.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = false;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest";

    @SuppressWarnings("WeakerAccess")
    static class Result {
        public String topic;
        public String payload;
    }

    private class Setting{
        private String groupId;
        private String topic;

        public String getGroupId() {
            return groupId;
        }

        public String getTopic() {
            return topic;
        }
    }

    public static void main(String[] args) {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        Gson gson = new Gson();

        try {
            String line = stdin.readLine();

            Setting obj = gson.fromJson(line, Setting.class);
            String groupId = obj.getGroupId();
            String topic = obj.getTopic();

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                System.out.println("start listening");
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tValue: " + record.value()
                    );
                    consumer.commitSync();
                    Result result = new Result();
                    result.topic = topic;
                    result.payload = record.value();
                    System.out.println("output");
                    System.out.println(gson.toJson(result));
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}