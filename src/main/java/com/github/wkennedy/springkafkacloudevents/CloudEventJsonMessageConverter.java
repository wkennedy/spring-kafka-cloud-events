package com.github.wkennedy.springkafkacloudevents;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Objects;

public class CloudEventJsonMessageConverter extends JsonMessageConverter {

    private static final EventFormat format = EventFormatProvider
            .getInstance()
            .resolveFormat(JsonFormat.CONTENT_TYPE);

    public CloudEventJsonMessageConverter() {
    }

    public CloudEventJsonMessageConverter(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
        //TODO this approach works well for binary encoding for cloud events, as the headers are mapped, but
        //needs some more work for structured type
        CloudEvent cloudEvent = (CloudEvent) record.value();
        JavaType javaType = getTypeMapper().getTypePrecedence().equals(Jackson2JavaTypeMapper.TypePrecedence.INFERRED)
                ? TypeFactory.defaultInstance().constructType(type)
                : getTypeMapper().toJavaType(record.headers());
        if (javaType == null) { // no headers
            javaType = TypeFactory.defaultInstance().constructType(type);
        }

        try {
            return getObjectMapper().readValue(cloudEvent.getData(), javaType);
        }
        catch (IOException e) {
            throw new ConversionException("Failed to convert from JSON", e);
        }
    }
}
