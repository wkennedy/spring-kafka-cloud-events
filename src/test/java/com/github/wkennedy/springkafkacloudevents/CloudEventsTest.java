package com.github.wkennedy.springkafkacloudevents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wkennedy.springkafkacloudevents.factory.CloudEventKafkaConsumerFactory;
import com.github.wkennedy.springkafkacloudevents.factory.CloudEventKafkaProducerFactory;
import com.github.wkennedy.springkafkacloudevents.models.Person;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.message.Encoding;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class CloudEventsTest {

    protected static final String cloudEventID = UUID.randomUUID().toString();
    protected static final String cloudEventType = "example.kafka";
    protected static final URI cloudEventSource = URI.create("http://localhost");
    protected static final String correlationId = UUID.randomUUID().toString();
    protected static final String causationId = UUID.randomUUID().toString();

    protected final static Person person = new Person("John", "Doe");
    private final ObjectMapper objectMapper = new ObjectMapper();

    protected CloudEvent getCloudEvent(Person data) {
        CloudEventData cloudEventData;
        try {
            cloudEventData = new BytesCloudEventData(objectMapper.writeValueAsBytes(data));
        } catch (JsonProcessingException e) {
            return null;
        }
        return CloudEventBuilder.v1()
                .withId(cloudEventID)
                .withType(cloudEventType)
                .withSource(cloudEventSource)
                .withExtension("correlationid", correlationId)
                .withExtension("causationid", causationId)
                .withData("application/json", cloudEventData)
                .build();
    }

    protected static DefaultKafkaProducerFactory<String, CloudEvent> producerFactory(Encoding encoding,
                                                                                     Map<String, Object> producerConfigs) {
        return CloudEventKafkaProducerFactory.cloudEventKafkaProducerFactory(producerConfigs,
                new StringSerializer(), encoding);
    }

    public static DefaultKafkaConsumerFactory<String, CloudEvent> consumerFactory(Encoding encoding,
                                                                                  Map<String, Object> consumerConfigs) {
        return CloudEventKafkaConsumerFactory.consumerFactory(consumerConfigs, new StringDeserializer(), encoding);
    }

}
