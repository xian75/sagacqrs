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

    // COMMIT      ROLLBACK
    // 0 -> null   0 -> 4
    // 1 -> 4      1 -> null
    // 2 -> 4      2 -> null
    // 3 -> null   3 -> 4
    INSERTING((short) 0),
    DELETING((short) 1),
    UPDATING((short) 2),
    UPDATE_TO_APPLY((short) 3),
    DELETED((short) 4);

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
