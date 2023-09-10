package datandart.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;




import java.time.Instant;
import java.util.Properties;

public class StockViewApp {
    public static void main(String[] args) {
        Properties config=new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"stock-view-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:30100");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // json Serde

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer()  ;
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);

        StreamsBuilder builder=new StreamsBuilder();

        //1 - Stream from Kafka
        KStream<String,JsonNode> volumeRecord=builder.stream("stock-input", Consumed.with(Serdes.String(), jsonSerde));

        KStream<String,JsonNode> countRecord=builder.stream("stock-input-count", Consumed.with(Serdes.String(), jsonSerde));

        //create json object
        ObjectNode initialVolume= JsonNodeFactory.instance.objectNode();
        initialVolume.put("Price",0.0);
        initialVolume.put("Volume",0.0);


        ObjectNode initialCount= JsonNodeFactory.instance.objectNode();
        initialCount.put("Count",0);


        //Ktables
        KTable<String,JsonNode> volumedata=volumeRecord
                //1 - Agrupanddo os valores
                .groupByKey()
                .aggregate(
                        ()->initialVolume,
                        (key,trade,stockposition)-> stockTrade(trade,stockposition),
                        Materialized.<String,JsonNode, KeyValueStore<Bytes,byte[]>>as("stock-volume-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        volumedata.toStream().to("stock-output", Produced.with(Serdes.String(),jsonSerde));

        KTable<String,JsonNode> countdata=countRecord
                //1 - Contando os valores
                .groupByKey()
                .aggregate(
                        ()->initialCount,
                        (key,count,stockcount)-> stockCounting(count,stockcount),
                        Materialized.<String,JsonNode, KeyValueStore<Bytes,byte[]>>as("count-volume-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
//
                );

        countdata.toStream().to("stock-output-count", Produced.with(Serdes.String(),jsonSerde));

        KafkaStreams streams=new KafkaStreams(builder.build(),config);

        streams.start();

        //print the topology
        System.out.println(streams.toString());

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


                                                                                                                                                                    }
    private static JsonNode stockTrade(JsonNode trade, JsonNode stockposition) {

        //create new trade json object
        ObjectNode newPosition=JsonNodeFactory.instance.objectNode();
        newPosition.put("Volume",stockposition.get("Volume").asDouble()+trade.get("size").asDouble());

        return newPosition;
    }
    private static JsonNode stockCounting(JsonNode count, JsonNode stockcount) {

        //create new count json object
        ObjectNode newCount=JsonNodeFactory.instance.objectNode();
        newCount.put("Count",stockcount.get("Count").asInt()+1);

        return newCount;
    }


}
