package it.noah.sagacqrs.entity.interfaces;

import it.noah.sagacqrs.entity.EventAttributes;
import it.noah.sagacqrs.enums.DatabaseEventState;

/**
 *
 * @author NATCRI
 */
public interface IEntity {

    public Long getId();

    public void setId(Long id);

    public Long getOptlock();

    public void setOptlock(Long optlock);

    public EventAttributes getEvent();

    public void setEvent(EventAttributes event);

    public default String getEventStateName() {
        if (getEvent() == null || getEvent().getState() == null) {
            return null;
        } else {
            return getEvent().getState().name();
        }
    }

    public default void markAsInserting(String uuid) {
        try {
            setEvent(EventAttributes.initialize(uuid, DatabaseEventState.INSERTING));
            setOptlock(0L);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public default void markAsDeleting(String uuid) {
        try {
            setEvent(EventAttributes.initialize(uuid, DatabaseEventState.DELETING));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public default void markAsUpdating(String uuid) {
        try {
            setEvent(EventAttributes.initialize(uuid, DatabaseEventState.UPDATING));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public default void markAsUpdateToApply(String uuid, Long id, Long optlock) {
        try {
            setEvent(EventAttributes.initialize(uuid, DatabaseEventState.UPDATE_TO_APPLY));
            setId(id);
            setOptlock(optlock + 1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
