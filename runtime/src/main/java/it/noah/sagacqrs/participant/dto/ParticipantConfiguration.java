/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.participant.dto;

import io.vertx.mutiny.sqlclient.Pool;
import it.noah.sagacqrs.entity.interfaces.IEntity;
import java.io.Serializable;
import java.util.List;
import org.jboss.logging.Logger;

/**
 *
 * @author NATCRI
 */
public class ParticipantConfiguration implements Serializable {

    private static final long serialVersionUID = 6754051537628608792L;

    private final Logger log;
    private final Pool dbPool;
    private final List<IEntity> tables;

    public ParticipantConfiguration(Logger log, Pool dbPool, List<IEntity> tables) {
        this.log = log;
        this.dbPool = dbPool;
        this.tables = tables;
    }

    public Logger getLog() {
        return log;
    }

    public Pool getDbPool() {
        return dbPool;
    }

    public List<IEntity> getTables() {
        return tables;
    }

}
