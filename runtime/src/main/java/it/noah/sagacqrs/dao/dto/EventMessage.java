/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.dao.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 *
 * @author NATCRI
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventMessage {

    private static final long serialVersionUID = -8281185096703890968L;

    private String uuid;
    private String operation;
    private OffsetDateTime expire;
    private String outcome;
    private ErrorMap errors;
    private Object details;

    public EventMessage() {
        uuid = UUID.randomUUID().toString();
        errors = new ErrorMap();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public OffsetDateTime getExpire() {
        return expire;
    }

    public void setExpire(OffsetDateTime expire) {
        this.expire = expire;
    }

    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
    }

    public ErrorMap getErrors() {
        return errors;
    }

    public void setErrors(ErrorMap errors) {
        this.errors = errors;
    }

    public Object getDetails() {
        return details;
    }

    public void setDetails(Object details) {
        this.details = details;
    }

}
