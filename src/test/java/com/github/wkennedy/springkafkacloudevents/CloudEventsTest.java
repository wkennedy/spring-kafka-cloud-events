package com.github.wkennedy.springkafkacloudevents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wkennedy.springkafkacloudevents.models.Person;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.Encoding;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = { "txCache1", "txCache2", "txCacheSendFromListener" },
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1" }
)
@SpringJUnitConfig
@DirtiesContext
public class CloudEventsTest {

    private static final String CONSUMER_GROUP_ID = "reactive_transaction_consumer_group";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    public void testContainerTxProducerIsNotCached() throws Exception {
        DefaultKafkaProducerFactory<String, CloudEvent> pf = producerFactory(Encoding.BINARY);
        KafkaTemplate<String, CloudEvent> template = new KafkaTemplate<>(pf);
        DefaultKafkaProducerFactory<String, CloudEvent> pfTx = producerFactory(Encoding.BINARY);
        pfTx.setTransactionIdPrefix("fooTx.");
        KafkaTemplate<String, CloudEvent> templateTx = new KafkaTemplate<>(pfTx);

        ConsumerFactory<String, CloudEvent> cf = consumerFactory(Encoding.BINARY);
        ContainerProperties containerProps = new ContainerProperties("txCache2");
        CountDownLatch latch = new CountDownLatch(1);
        containerProps.setMessageListener((MessageListener<String, CloudEvent>) r -> {
            templateTx.send("txCacheSendFromListener", getCloudEvent(new Person("John", "Doe")));
            templateTx.send("txCacheSendFromListener", getCloudEvent(new Person("Jane", "Doe")));
            latch.countDown();
        });
        KafkaTransactionManager<String, CloudEvent> tm = new KafkaTransactionManager<>(pfTx);
        containerProps.setTransactionManager(tm);
        KafkaMessageListenerContainer<String, CloudEvent> container = new KafkaMessageListenerContainer<>(cf,
                containerProps);
        container.start();
        try {
            ListenableFuture<SendResult<String, CloudEvent>> future = template.send("txCache2", getCloudEvent(new Person("Test", "Account")));
            SendResult<String, CloudEvent> stringCloudEventSendResult = future.get(10, TimeUnit.SECONDS);
            System.out.println("RESULT:::: " + stringCloudEventSendResult.toString());
            assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(KafkaTestUtils.getPropertyValue(pfTx, "cache", Map.class)).hasSize(0);
        }
        finally {
            container.stop();
            pf.destroy();
            pfTx.destroy();
        }
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

    public DefaultKafkaProducerFactory<String, CloudEvent> producerFactory(Encoding encoding) {
        return CloudEventKafkaProducerFactory.cloudEventKafkaProducerFactory(producerConfigs(),
                new StringSerializer(), encoding);
    }

    public Map<String, Object> producerConfigs() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafka);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);
        return producerProps;
    }


    public DefaultKafkaConsumerFactory<String, CloudEvent> consumerFactory(Encoding encoding) {
        return CloudEventKafkaConsumerFactory.consumerFactory(consumerConfigs(), new StringDeserializer(), encoding);
    }

    public Map<String, Object> consumerConfigs() {
        return KafkaTestUtils.consumerProps("txCache2Group", "false", this.embeddedKafka);
    }
}
