/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.dao.dto;

import it.noah.sagacqrs.participant.interfaces.IParticipantClient;
import java.io.Serializable;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 *
 * @author NATCRI
 */
public class Participant implements Serializable {

    private static final long serialVersionUID = -1601701156493344681L;

    private final IParticipantClient client;
    private final String name;
    private long sagaTimeout;
    private long cqrsTimeout;

    public Participant(IParticipantClient client) {
        this.client = client;
        name = client.getName();
        sagaTimeout = getSagaTimeoutByPropertyOrDefault();
        cqrsTimeout = getCqrsTimeoutByPropertyOrDefault();
    }

    public Participant(IParticipantClient client, Long sagaTimeout, Long cqrsTimeout) {
        this.client = client;
        name = client.getName();
        if (sagaTimeout != null) {
            this.sagaTimeout = sagaTimeout;
        } else {
            this.sagaTimeout = getSagaTimeoutByPropertyOrDefault();
        }
        if (cqrsTimeout != null) {
            this.cqrsTimeout = cqrsTimeout;
        } else {
            this.cqrsTimeout = getCqrsTimeoutByPropertyOrDefault();
        }
    }

    public IParticipantClient getClient() {
        return client;
    }

    public String getName() {
        return name;
    }

    public long getSagaTimeout() {
        return sagaTimeout;
    }

    public void setSagaTimeout(long sagaTimeout) {
        this.sagaTimeout = sagaTimeout;
    }

    public long getCqrsTimeout() {
        return cqrsTimeout;
    }

    public void setCqrsTimeout(long cqrsTimeout) {
        this.cqrsTimeout = cqrsTimeout;
    }

    private long getSagaTimeoutByPropertyOrDefault() {
        try {
            return ConfigProvider.getConfig().getValue("saga.participant.timeout", Long.class);
        } catch (NumberFormatException ex) {
            return 10000L;
        }
    }

    private long getCqrsTimeoutByPropertyOrDefault() {
        try {
            return ConfigProvider.getConfig().getValue("cqrs.participant.timeout", Long.class);
        } catch (NumberFormatException ex) {
            return 30000L;
        }
    }
}
