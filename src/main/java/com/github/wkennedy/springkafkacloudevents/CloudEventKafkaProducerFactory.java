package com.github.wkennedy.springkafkacloudevents;

import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.cloudevents.kafka.CloudEventSerializer.ENCODING_CONFIG;
import static io.cloudevents.kafka.CloudEventSerializer.EVENT_FORMAT_CONFIG;

public class CloudEventKafkaProducerFactory {

    @SuppressWarnings("unchecked")
    public static <K, V> DefaultKafkaProducerFactory<K, V> cloudEventKafkaProducerFactory(Map<String, Object> configs,
                                                                   Serializer<K> keySerializer,
                                                                   Encoding encoding) {
        Map<String, Object> ceSerializerConfigs = new HashMap<>();
        ceSerializerConfigs.put(ENCODING_CONFIG, encoding);
        ceSerializerConfigs.put(EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);
        CloudEventSerializer cloudEventSerializer = new CloudEventSerializer();
        cloudEventSerializer.configure(ceSerializerConfigs, false);//isKey always false

        return new DefaultKafkaProducerFactory<>(configs, keySerializer, (Serializer<V>) cloudEventSerializer);
    }
}
