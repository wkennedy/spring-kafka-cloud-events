package com.github.wkennedy.springkafkacloudevents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wkennedy.springkafkacloudevents.models.Person;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class CloudEventJsonMessageConverterTest {

    CloudEventJsonMessageConverter cloudEventJsonMessageConverter = new CloudEventJsonMessageConverter();
    private static final EventFormat format = EventFormatProvider
            .getInstance()
            .resolveFormat(JsonFormat.CONTENT_TYPE);

    private final Person person = new Person("John", "Doe");
    private final CloudEvent cloudEvent = getCloudEvent(person);

    @Test
    void toMessage() {
        ConsumerRecord<String, CloudEvent> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0L, 0, 0, "bar", cloudEvent);
        Message<?> message = cloudEventJsonMessageConverter.toMessage(record, null, null, Person.class);
        assertThat((Person)message.getPayload()).isEqualTo(person);
        assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
        assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isEqualTo("bar");
    }

    private CloudEvent getCloudEvent(Person data) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return CloudEventBuilder.v1()
                    .withId("hello")
                    .withType("example.kafka")
                    .withSource(URI.create("http://localhost"))
                    .withData("application/json", objectMapper.writeValueAsBytes(data))
                    .build();
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}