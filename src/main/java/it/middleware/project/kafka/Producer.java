package it.middleware.project.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {

    @SuppressWarnings("WeakerAccess")
    static class Result {
        public String topic;
        public String payload;
    }

    private static final boolean waitAck = true;

    private static String serverAddr;

    private class Topic{
        private String topic;
        private String payload;

        public String getTopic() {
            return topic;
        }

        public String getPayload() {
            return payload;
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {

        try {
            //InputStream is = Consumer.class.getClassLoader().getResourceAsStream("address.txt");
            BufferedReader br = new BufferedReader(new FileReader("address.txt"));
            serverAddr = br.readLine();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        Gson gson = new Gson();
        System.out.println(serverAddr);

        while (true) {
            try {
                String line = stdin.readLine();
                Topic obj = gson.fromJson(line, Topic.class);

                String topic = obj.getTopic();
                String payload = obj.getPayload();

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

                Result result = new Result();
                result.topic = topic;
                result.payload = record.value();
                System.out.println(gson.toJson(result));

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