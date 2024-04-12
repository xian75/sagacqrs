package it.noah.sagacqrs.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import it.noah.sagacqrs.json.OffsetDateTimeDeserializer;
import it.noah.sagacqrs.json.OffsetDateTimeSerializer;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.Serializable;
import java.time.OffsetDateTime;
import org.jboss.logging.Logger;

/**
 *
 * @author NATCRI
 */
@ApplicationScoped
public class Jsoner implements Serializable {

    private static final long serialVersionUID = -2082681740124552876L;

    static final ObjectMapper basicExchangeableMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    static final ObjectMapper mapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .addModule(new Jdk8Module())
            .addModule(new ParameterNamesModule())
            .build()
            //.enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .registerModule(new SimpleModule().addSerializer(OffsetDateTime.class, new OffsetDateTimeSerializer()))
            .registerModule(new SimpleModule().addDeserializer(OffsetDateTime.class, new OffsetDateTimeDeserializer()));

    public String json(Logger log, Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException ex) {
            log.error(ex);
            return null;
        }
    }

    public <T> T getObject(Object source, Class<T> clazz) {
        return basicExchangeableMapper.convertValue(source, clazz);
    }
}
