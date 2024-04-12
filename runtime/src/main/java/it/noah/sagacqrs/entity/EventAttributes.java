/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.entity;

import io.vertx.mutiny.sqlclient.Row;
import it.noah.sagacqrs.enums.DatabaseEventState;

/**
 *
 * @author NATCRI
 */
public class EventAttributes {

    private static final long serialVersionUID = -2472850651649886387L;

    protected String uuid;
    protected DatabaseEventState state;

    public static EventAttributes initialize(String uuid, DatabaseEventState state) {
        EventAttributes event = new EventAttributes();
        event.setUuid(uuid);
        event.setState(state);
        return event;
    }

    public static EventAttributes initialize(Row row) {
        EventAttributes event = new EventAttributes();
        event.setUuid(row.getString("event_uuid"));
        event.setState(DatabaseEventState.getById(row.getShort("event_state")));
        return event;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public DatabaseEventState getState() {
        return state;
    }

    public void setState(DatabaseEventState state) {
        this.state = state;
    }

}
