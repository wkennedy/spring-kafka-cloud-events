package com.github.wkennedy.springkafkacloudevents;

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

@EmbeddedKafka(topics = { BinaryCloudEventsTest.TOPIC },
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1" }
)
public class BinaryCloudEventsTest extends CloudEventsTest {
    public static final String TOPIC = "BINARY_TOPIC";

    private static EmbeddedKafkaBroker embeddedKafka;

    private static DefaultKafkaProducerFactory<String, CloudEvent> binaryProducerFactory;
    private static KafkaTemplate<String, CloudEvent> binaryKafkaTemplate;

    private static Consumer<String, CloudEvent> binaryConsumer;

    private static Map<String, Object> producerProps;
    private static Map<String, Object> consumerProps;

    @BeforeAll
    public static void setUp() {
        embeddedKafka = EmbeddedKafkaCondition.getBroker();
        consumerProps = KafkaTestUtils.consumerProps("binaryTestGroup", "false", embeddedKafka);

        producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);

        binaryProducerFactory = producerFactory(Encoding.BINARY, producerProps);
        binaryKafkaTemplate = new KafkaTemplate<>(binaryProducerFactory);

        ConsumerFactory<String, CloudEvent> binaryConsumerFactory = consumerFactory(Encoding.BINARY, consumerProps);
        binaryConsumer = binaryConsumerFactory.createConsumer();

        embeddedKafka.consumeFromAnEmbeddedTopic(binaryConsumer, TOPIC);
    }

    @AfterAll
    public static void tearDown() {
        binaryConsumer.close();
        binaryProducerFactory.destroy();
    }

    @Test
    public void testCloudEventsBinaryMessageConverter() throws Exception {
        binaryKafkaTemplate.send(TOPIC, getCloudEvent(person));
        ConsumerRecord<String, CloudEvent> consumerRecord = KafkaTestUtils.getSingleRecord(binaryConsumer, TOPIC);
        CloudEventJsonMessageConverter messageConverter = new CloudEventJsonMessageConverter();
        Acknowledgment ack = mock(Acknowledgment.class);
        Consumer<?, ?> mockConsumer = mock(Consumer.class);
        Message<?> message = messageConverter.toMessage(consumerRecord, ack, mockConsumer, Person.class);
        Person payload = (Person) message.getPayload();

        assertThat(new String((byte[])message.getHeaders().get("ce_id"))).isEqualTo(cloudEventID);
        assertThat(new String((byte[])message.getHeaders().get("ce_type"))).isEqualTo(cloudEventType);
        assertThat(new URI(new String((byte[])message.getHeaders().get("ce_source")))).isEqualTo(cloudEventSource);

        assertThat(payload).isEqualTo(person);

        KafkaUtils.clearConsumerGroupId();
    }

}
