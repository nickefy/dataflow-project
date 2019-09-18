package my.mudah.beam.test;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class MyClassKafkaAvroDeserializer extends
AbstractKafkaAvroDeserializer implements Deserializer<action_states_pkey> {

@Override
public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new KafkaAvroDeserializerConfig(configs));
}

@Override
public action_states_pkey deserialize(String s, byte[] bytes) {
    return (action_states_pkey) this.deserialize(bytes);
}

@Override
public void close() {} }