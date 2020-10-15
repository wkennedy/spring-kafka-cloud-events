package com.github.wkennedy.springkafkacloudevents;

import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.cloudevents.kafka.CloudEventSerializer.ENCODING_CONFIG;
import static io.cloudevents.kafka.CloudEventSerializer.EVENT_FORMAT_CONFIG;

public class CloudEventKafkaConsumerFactory {

    public static <K, V> DefaultKafkaConsumerFactory<K, V> consumerFactory(Map<String, Object> configs,
                                                               Deserializer<K> keySerializer,
                                                               Encoding encoding) {
        return consumerFactory(configs, keySerializer, encoding, JsonFormat.CONTENT_TYPE);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DefaultKafkaConsumerFactory<K, V> consumerFactory(Map<String, Object> configs,
                                                                           Deserializer<K> keySerializer,
                                                                           Encoding encoding, String eventFormat) {
        Map<String, Object> ceDeserializerConfigs = new HashMap<>();
        ceDeserializerConfigs.put(ENCODING_CONFIG, encoding);
        ceDeserializerConfigs.put(EVENT_FORMAT_CONFIG, eventFormat);

        CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();
        cloudEventDeserializer.configure(ceDeserializerConfigs, false); //isKey always false
        return new DefaultKafkaConsumerFactory<>(configs,
                keySerializer,
                (Deserializer<V>) cloudEventDeserializer);
    }
}
