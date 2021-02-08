package it.middleware.project.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {

    private static final boolean waitAck = true;

    private static final String serverAddr = "localhost:9092";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        Gson gson = new Gson();

        while (true) {
            try {
                String line = stdin.readLine();
                JsonObject obj = gson.fromJson(line, JsonObject.class);
                String topic = obj.get("topic").toString().replace("\"", "");
                String payload = obj.get("payload").toString();

                final Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                // Idempotence = exactly once semantics between the producer and the partition
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

                final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                System.out.println(
                        "Topic: " + topic +
                                "\tValue: " + payload
                );

                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);
                final Future<RecordMetadata> future = producer.send(record);

                if (waitAck) {
                    try {
                        RecordMetadata ack = future.get();
                        System.out.println("Ack for topic " + ack.topic() + ", partition " + ack.partition() + ", offset " + ack.offset());
                    } catch (InterruptedException | ExecutionException e1) {
                        e1.printStackTrace();
                    }
                }

                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}