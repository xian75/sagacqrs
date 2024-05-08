/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.enums;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author NATCRI
 */
public enum DatabaseEventState implements Serializable {

    // COMMIT       ROLLBACK
    // 0 -> null    0 -> CANCEL
    // 1 -> -1      1 -> null
    // 2 -> CANCEL  2 -> null
    // 3 -> -1      3 -> null
    // 4 -> CANCEL  4 -> null
    // 5 -> null    5 -> CANCEL
    DELETED((short) -1),
    INSERTING((short) 0),
    LOGICAL_DELETING((short) 1),
    DELETING((short) 2),
    UPDATE_ARCHIVING((short) 3),
    UPDATING((short) 4),
    UPDATE_TO_APPLY((short) 5);

    private final short id;

    private static final Map<Short, DatabaseEventState> map = new HashMap();

    static {
        for (DatabaseEventState type : DatabaseEventState.values()) {
            map.put(type.getId(), type);
        }
    }

    private DatabaseEventState(short id) {
        this.id = id;
    }

    public short getId() {
        return id;
    }

    public static DatabaseEventState getById(Short id) {
        return map.get(id);
    }
}
