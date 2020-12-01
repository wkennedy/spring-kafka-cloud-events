package com.github.wkennedy.springkafkacloudevents;

import com.github.wkennedy.springkafkacloudevents.converter.CloudEventJsonMessageConverter;
import com.github.wkennedy.springkafkacloudevents.models.Person;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@EmbeddedKafka(topics = { StructuredCloudEventsTest.TOPIC },
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1" }
)
public class StructuredCloudEventsTest extends CloudEventsTest {
    public static final String TOPIC = "STRUCTURED_TOPIC";

    private static EmbeddedKafkaBroker embeddedKafka;

    private static DefaultKafkaProducerFactory<String, CloudEvent> structuredProducerFactory;
    private static KafkaTemplate<String, CloudEvent> structuredKafkaTemplate;

    private static Consumer<String, CloudEvent> structuredConsumer;

    private static Map<String, Object> producerProps;
    private static Map<String, Object> consumerProps;

    @BeforeAll
    public static void setUp() {
        embeddedKafka = EmbeddedKafkaCondition.getBroker();
        consumerProps = KafkaTestUtils.consumerProps("structuredTestGroup", "false", embeddedKafka);

        producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);

        structuredProducerFactory = producerFactory(Encoding.STRUCTURED, producerProps);
        structuredKafkaTemplate = new KafkaTemplate<>(structuredProducerFactory);

        ConsumerFactory<String, CloudEvent> structuredConsumerFactory = consumerFactory(Encoding.STRUCTURED, consumerProps);
        structuredConsumer = structuredConsumerFactory.createConsumer();

        embeddedKafka.consumeFromAnEmbeddedTopic(structuredConsumer, TOPIC);
    }

    @AfterAll
    public static void tearDown() {
        structuredConsumer.close();
        structuredProducerFactory.destroy();
    }
    
    //TODO structured cloud event loses metadata in message converter
    @Test
    public void testCloudEventsStructuredMessageConverter() {
        DefaultKafkaProducerFactory<String, CloudEvent> pf = producerFactory(Encoding.STRUCTURED, producerProps);
        KafkaTemplate<String, CloudEvent> template = new KafkaTemplate<>(pf);

        template.send(TOPIC, getCloudEvent(person));
        ConsumerRecord<String, CloudEvent> consumerRecord = KafkaTestUtils.getSingleRecord(structuredConsumer, TOPIC);
        CloudEventJsonMessageConverter messageConverter = new CloudEventJsonMessageConverter();
        Acknowledgment ack = mock(Acknowledgment.class);
        Consumer<?, ?> mockConsumer = mock(Consumer.class);

        CloudEvent cloudEvent = consumerRecord.value();
        assertThat(cloudEvent.getId()).isEqualTo(cloudEventID);
        assertThat(cloudEvent.getType()).isEqualTo(cloudEventType);
        assertThat(cloudEvent.getSource()).isEqualTo(cloudEventSource);
        assertThat(cloudEvent.getExtension("correlationid")).isEqualTo(correlationId);
        assertThat(cloudEvent.getExtension("causationid")).isEqualTo(causationId);

        //Test message converter for structured content type
        Message<?> message = messageConverter.toMessage(consumerRecord, ack, mockConsumer, Person.class);
        Person payload = (Person) message.getPayload();
        assertThat(payload).isEqualTo(person);

        KafkaUtils.clearConsumerGroupId();
    }
}
