package it.noah.sagacqrs.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.vertx.mutiny.sqlclient.Row;
import java.io.Serializable;
import java.time.OffsetDateTime;

/**
 *
 * @author NATCRI
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1278062676608645416L;

    private String uuid;
    private String operation;
    private String outcome;
    private OffsetDateTime expire;
    private Long timeout;

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

    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
    }

    public OffsetDateTime getExpire() {
        return expire;
    }

    public void setExpire(OffsetDateTime expire) {
        this.expire = expire;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Transaction from(Row row) {
        setUuid(row.getString("event_uuid"));
        setOperation(row.getString("operation"));
        setOutcome(row.getString("outcome"));
        setExpire(row.getOffsetDateTime("expire"));
        setTimeout(row.getLong("timeout"));
        return this;
    }

    public Transaction fromToFinalize(Row row) {
        setUuid(row.getString("event_uuid"));
        setOutcome(row.getString("outcome"));
        return this;
    }

    public Object[] array() {
        return new Object[]{
            getUuid(), getOperation(), getOutcome(), getExpire(), getTimeout()};
    }

}
