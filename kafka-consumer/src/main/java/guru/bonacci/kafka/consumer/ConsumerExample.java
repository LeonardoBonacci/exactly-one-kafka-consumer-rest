package guru.bonacci.kafka.consumer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConsumerExample {

    public static void main(final String[] args) throws Exception {
        final var topic = "slow";

        final var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "turtles-all-the-way-down");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "turtles-all-the-way-down");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        final Consumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Arrays.asList(topic),
                new ConsumerRebalanceListener() {

        			@Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        System.out.println("onPartitionsRevoked");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        System.out.println("onPartitionsAssigned");
                    	Map<Integer, Long> offsets = getOffsets();
                    	System.out.println(offsets);
                    	for (TopicPartition partition : partitions) {
                    		long p = offsets.containsKey(partition.partition()) ? offsets.get(partition.partition()) : 0l;
                    		consumer.seek(partition, p);
                    	}
                    }
                });
        
        try {
            while (true) {
            	var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    String key = record.key();
                    String value = record.value();

                    System.out.println(
                        String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));

                    sendSomething(record);
                    
                    System.out.println(
                            String.format("Processed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        } finally {
            consumer.close();
        }
    }

    static void sendSomething(ConsumerRecord<String, String> record) {
		try {
	    	final var client = HttpClient.newBuilder().build();
		    	final var postRequest = HttpRequest.newBuilder()
			    			.uri(new URI("http://localhost:8080"))
			    	    	.headers("partition", String.valueOf(record.partition()), "offset", String.valueOf(record.offset()))
			    			.POST(BodyPublishers.ofString(record.value()))
			    			.build();

			    	HttpResponse<String> resp = client.send(postRequest, BodyHandlers.ofString());
			    	System.out.println("all good? " + resp.statusCode());
					
				client.send(postRequest, BodyHandlers.ofString());
		} catch (IOException | InterruptedException | URISyntaxException e) {
			e.printStackTrace();
		}
    }

    static Map<Integer, Long> getOffsets() {
		try {
	    	final var client = HttpClient.newBuilder().build();
	    	final var getRequest = HttpRequest.newBuilder()
	    			.uri(new URI("http://localhost:8080"))
	    	    	.GET()
	    			.build();
	
	    	String json = client.send(getRequest, HttpResponse.BodyHandlers.ofString()).body();
	    	return new ObjectMapper().readValue(json, Map.class);
		} catch (IOException | InterruptedException | URISyntaxException e) {
			e.printStackTrace();
		}
		return null; 
    }
}