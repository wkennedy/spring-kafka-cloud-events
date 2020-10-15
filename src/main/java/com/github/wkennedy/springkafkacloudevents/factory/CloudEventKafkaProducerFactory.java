package com.github.wkennedy.springkafkacloudevents.factory;

import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.cloudevents.kafka.CloudEventSerializer.ENCODING_CONFIG;
import static io.cloudevents.kafka.CloudEventSerializer.EVENT_FORMAT_CONFIG;

public class CloudEventKafkaProducerFactory {

    public static <K, V> DefaultKafkaProducerFactory<K, V> cloudEventKafkaProducerFactory(Map<String, Object> configs,
                                                                   Serializer<K> keySerializer,
                                                                   Encoding encoding) {
        return cloudEventKafkaProducerFactory(configs, keySerializer, encoding, JsonFormat.CONTENT_TYPE);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DefaultKafkaProducerFactory<K, V> cloudEventKafkaProducerFactory(Map<String, Object> configs,
                                                                                          Serializer<K> keySerializer,
                                                                                          Encoding encoding,
                                                                                          String eventFormat) {
        //If present, the Kafka message header property content-type MUST be set to the media type of an event format.
        if(Encoding.STRUCTURED.equals(encoding)) {
            EventFormat resolveFormat = EventFormatProvider.getInstance().resolveFormat(eventFormat);
            if(resolveFormat == null) {
                eventFormat = JsonFormat.CONTENT_TYPE;
            }
        }
        Map<String, Object> ceSerializerConfigs = new HashMap<>();
        ceSerializerConfigs.put(ENCODING_CONFIG, encoding);
        ceSerializerConfigs.put(EVENT_FORMAT_CONFIG, eventFormat);
        CloudEventSerializer cloudEventSerializer = new CloudEventSerializer();
        cloudEventSerializer.configure(ceSerializerConfigs, false); //isKey always false

        return new DefaultKafkaProducerFactory<>(configs, keySerializer, (Serializer<V>) cloudEventSerializer);
    }
}
