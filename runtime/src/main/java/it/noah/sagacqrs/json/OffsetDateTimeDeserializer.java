/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 *
 * @author NATCRI
 */
public class OffsetDateTimeDeserializer extends JsonDeserializer<OffsetDateTime> {

    @Override
    public OffsetDateTime deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        String dateAsString = jsonParser.getText();
        if (dateAsString != null) {
            LocalDateTime localDateTime = LocalDateTime.parse(dateAsString, DateTimeFormatter.ofPattern(getConfigProperty("sagacqrs.log.datetimeformat")));
            ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
            return localDateTime.atOffset(zoneOffset);
        } else {
            return null;
        }
    }

    private String getConfigProperty(String property) {
        try {
            return ConfigProvider.getConfig().getValue(property, String.class);
        } catch (Exception ex) {
            return "MM/dd/yyyy HH:mm:ss.SSSSSS";
        }
    }

}
