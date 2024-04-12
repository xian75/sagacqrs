/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 *
 * @author NATCRI
 */
public class OffsetDateTimeSerializer extends JsonSerializer<OffsetDateTime> {

    @Override
    public void serialize(OffsetDateTime value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        if (value != null) {
            LocalDateTime ldt = value.atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
            jsonGenerator.writeString(DateTimeFormatter.ofPattern(getConfigProperty("sagacqrs.log.datetimeformat")).format(ldt));
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
