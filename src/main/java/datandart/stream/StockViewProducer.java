package datandart.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;


//for file reading


public class StockViewProducer {
    public static void main(String[] args) {
        Properties config=new Properties();

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ade8593de0e624b3a9537a47dacaeaf0-1278428698.us-east-2.elb.amazonaws.com:9092");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        config.put(ProducerConfig.ACKS_CONFIG,"all");
        config.put(ProducerConfig.RETRIES_CONFIG,"3");
        config.put(ProducerConfig.LINGER_MS_CONFIG,"1");

        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        Producer<String,String> producer=new KafkaProducer<>(config);

        int i=0;

        ObjectMapper mapper = new ObjectMapper();

        try {
            File myObj = new File("dados");
            Scanner trade = new Scanner(myObj);

            while (trade.hasNextLine()) {
                JsonNode data = mapper.readTree(trade.nextLine());
                System.out.println(data.toString());
                System.out.println(data.get("p").toString());
                producer.send(newTrade(data.get("p").toString(),data.toString()));
                i+=1;
                Thread.sleep(100);
            }
            trade.close();
        } catch (Exception e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        producer.close();
    }
    private static ProducerRecord<String,String> newTrade(String keyprice,String tradedt) {
        // Irá ler a linha do arquivo e gerar o key no preço para ser enviado para o producer do Kafka

        return new ProducerRecord<>("stock-input",keyprice,tradedt.toString());

    }




}
