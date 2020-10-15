# spring-kafka-cloud-events

![Java CI with Maven](https://github.com/wkennedy/spring-kafka-cloud-events/workflows/Java%20CI%20with%20Maven/badge.svg)

Cloud-Events Kafka Binding with Spring

The spring-kafka-cloud-events library provides utility in order to use the CloudEvents spec in your project that uses Spring-Kafka. The CloudEvents specification describes event data in a common way and provides both a Java-SDK as well as a library for Kafka protocol binding.

The CloudEvents kafka protocol binding provides two content modes for transferring data.

- Structured: event metadata attributes and event data are placed into the Kafka message value section using an event format.

- Binary: the value of the event data MUST be placed into the Kafka message's value section as-is, with the content-type header value declaring its media type; all other event attributes MUST be mapped to the Kafka message's header section.

For more details see: https://github.com/cloudevents/spec/blob/master/kafka-protocol-binding.md#13-content-modes

#### Binary producer and consumer

**Producer**

You can create a ProducerFactor to serialize your data into a structured cloud event using the CloudEventKafkaProducerFactory. You simply need to provide it with your producer configs, the key serializer, and the encoding type, in this case, Encoding.STRUCTURED.

    DefaultKafkaProducerFactory<String, CloudEvent> binaryProducerFactory =
    CloudEventKafkaProducerFactory.cloudEventKafkaProducerFactory(
    producerConfigs, new StringSerializer(), Encoding.BINARY);


**Consumer**

The process of creating a consumer is similar to that of the producer. 

    ConsumerFactory<String, CloudEvent> binaryConsumerFactory = CloudEventKafkaConsumerFactory.consumerFactory(consumerConfigs, new StringDeserializer(), Encoding.BINARY)
    
#### Structured producer and consumer

**Producer**

You can create a ProducerFactor to serialize your data into a structured cloud event using the CloudEventKafkaProducerFactory. You simply need to provide it with your producer configs, the key serializer, and the encoding type, in this case, Encoding.STRUCTURED.

    DefaultKafkaProducerFactory<String, CloudEvent> structuredProducerFactory =
    CloudEventKafkaProducerFactory.cloudEventKafkaProducerFactory(
    producerConfigs, new StringSerializer(), Encoding.STRUCTURED);


**Consumer**

The process of creating a consumer is similar to that of the producer. 

    ConsumerFactory<String, CloudEvent> structuredConsumerFactory = CloudEventKafkaConsumerFactory.consumerFactory(consumerConfigs, new StringDeserializer(), Encoding.STRUCTURED)
    
**Example Usage**

    DefaultKafkaProducerFactory<String, CloudEvent> binaryProducerFactory = CloudEventKafkaProducerFactory.cloudEventKafkaProducerFactory(
                producerProps,
                new StringSerializer(),
                Encoding.BINARY);
    KafkaTemplate<String, CloudEvent> binaryKafkaTemplate = new KafkaTemplate<>(binaryProducerFactory);

    ConsumerFactory<String, CloudEvent> binaryConsumerFactory = CloudEventKafkaConsumerFactory.consumerFactory(consumerProps, new StringDeserializer(), Encoding.BINARY);//consumerFactory(Encoding.BINARY, consumerProps);
    Consumer<String, CloudEvent> binaryConsumer = binaryConsumerFactory.createConsumer();

#### CloudEventJsonMessageConverter

This class will extract the JSON data payload from your CloudEvent and return a Spring Message<?> for your specified type. If you are using the binary content mode, then the CloudEvent metadata will persist in your Spring Message headers. However, if using the structured content mode, you will lose that information and need to retrieve it from the ConsumerRecord.

**Example**

    ConsumerRecord<String, CloudEvent> consumerRecord = KafkaTestUtils.getSingleRecord(binaryConsumer, TOPIC);
    CloudEventJsonMessageConverter messageConverter = new CloudEventJsonMessageConverter();
    Message<?> message = messageConverter.toMessage(consumerRecord, ack, mockConsumer, Person.class);
    Person payload = (Person) message.getPayload();
    String cloudEventId = new String((byte[])message.getHeaders().get("ce_id"));
    
#### Sources

https://cloudevents.io/

https://github.com/cloudevents

https://github.com/cloudevents/spec/blob/master/kafka-protocol-binding.md

https://github.com/spring-projects/spring-kafka
