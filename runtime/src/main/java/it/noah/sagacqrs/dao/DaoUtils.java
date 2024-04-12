package it.noah.sagacqrs.dao;

import it.noah.sagacqrs.dao.interfaces.ICommonDao;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.SqlConnection;
import it.noah.sagacqrs.enums.DatabaseEventState;
import it.noah.sagacqrs.dao.dto.QueryParameters;
import it.noah.sagacqrs.dao.dto.QueryResponse;
import it.noah.sagacqrs.entity.interfaces.IEntity;
import it.noah.sagacqrs.json.Jsoner;
import it.noah.sagacqrs.participant.EntitiesConfigurator;
import it.noah.sagacqrs.participant.dto.EntityConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 *
 * @author NATCRI
 */
@ApplicationScoped
public class DaoUtils implements ICommonDao {

    @Inject
    private EntitiesConfigurator entitiesConfigurator;

    @Inject
    Jsoner jsoner;

    public <T extends IEntity> Uni<T> find(Logger log, SqlConnection conn, T responseObject, Long id) {
        return executeGetResultList(log, conn, String.format("SELECT * FROM %s WHERE id = $1",
                getEntityConfig(log, responseObject).dbTable()), new Object[]{id}, responseObject, false, true)
                .onItem().transform(items -> items.get(0));
    }

    public <T extends IEntity> Uni<List<T>> findWithDeleted(Logger log, SqlConnection conn, T responseObject, Long id) {
        return executeGetResultList(log, conn, String.format("SELECT * FROM %s WHERE id = $1",
                getEntityConfig(log, responseObject).dbTable()), new Object[]{id}, responseObject, true, false);
    }

    public <T extends IEntity> Uni<List<T>> getResultList(Logger log, SqlConnection conn, String query, T responseObject) {
        return executeGetResultList(log, conn, query, responseObject, false, false);
    }

    public <T extends IEntity> Uni<List<T>> getResultList(Logger log, SqlConnection conn, String query, Object[] parameters, T responseObject) {
        return executeGetResultList(log, conn, query, parameters, responseObject, false, false);
    }

    public <T extends IEntity> Uni<List<T>> getResultListWithDeleted(Logger log, SqlConnection conn, String query, T responseObject) {
        return executeGetResultList(log, conn, query, responseObject, true, false);
    }

    public <T extends IEntity> Uni<List<T>> getResultListWithDeleted(Logger log, SqlConnection conn, String query, Object[] parameters, T responseObject) {
        return executeGetResultList(log, conn, query, parameters, responseObject, true, false);
    }

    public <T extends IEntity> Uni<T> persist(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, T e) {
        e.markAsInserting(uuid);
        return executeOnePersist(log, conn, expire, e);
    }

    public <T extends IEntity> Uni<List<T>> persist(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, List<T> items) {
        for (T item : items) {
            item.markAsInserting(uuid);
        }
        return executeManyPersist(log, conn, expire, items);
    }

    public <T extends IEntity> Uni<T> remove(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, T e, Long requiredOptlock) {
        check(log, uuid, e, requiredOptlock, true);
        DatabaseEventState originalState = e.getEvent().getState();
        e.markAsDeleting(uuid);
        return executeOneMergeOrRemove(log, conn, uuid, expire, e, requiredOptlock, originalState);
    }

    public <T extends IEntity> Uni<List<T>> remove(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, List<T> items) {
        check(log, uuid, items);
        final List<T> itemsToRemove = removeDeletedItems(items);
        Map<String, String> schemaTableIdMap = new HashMap<>();
        Map<String, T> schemaTableEntityMap = new HashMap<>();
        Map<String, Map<Long, Long>> schemaTableOptlockMap = new HashMap<>();
        for (T item : itemsToRemove) {
            item.markAsDeleting(uuid);
            String schemaTable = getEntityConfig(log, item).dbTable();
            if (!schemaTableIdMap.containsKey(schemaTable)) {
                schemaTableIdMap.put(schemaTable, "" + item.getId());
                try {
                    schemaTableEntityMap.put(schemaTable, (T) getEntityConfig(log, item).newInstance());
                } catch (Throwable ex) {
                    log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + item.getClass().getSimpleName() + " CONFIGURATION"), ex);
                    throw new RuntimeException("Missing or wrong entity " + item.getClass().getSimpleName() + " configuration");
                }
                schemaTableOptlockMap.put(schemaTable, new HashMap<>());
            } else {
                schemaTableIdMap.put(schemaTable, schemaTableIdMap.get(schemaTable) + "," + item.getId());
            }
            schemaTableOptlockMap.get(schemaTable).put(item.getId(), item.getOptlock());
        }
        return executeManyMergeOrRemove(log, conn, uuid, expire, schemaTableIdMap, schemaTableEntityMap,
                schemaTableOptlockMap, itemsToRemove.size(), DatabaseEventState.DELETING)
                .map((v) -> itemsToRemove);
    }

    public <T extends IEntity> Uni<T> merge(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, T o, T n, Long requiredOptlock) {
        check(log, uuid, o, requiredOptlock, false);
        DatabaseEventState originalState = o.getEvent().getState();
        o.markAsUpdating(uuid);
        return executeOneMergeOrRemove(log, conn, uuid, expire, o, requiredOptlock, originalState).chain(() -> {
            n.markAsUpdateToApply(uuid, o.getId(), o.getOptlock());
            return executeOnePersist(log, conn, expire, n);
        });
    }

    public <T extends IEntity> Uni<List<T>> merge(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, List<T> olds, Map<Long, T> news) {
        check(log, uuid, olds);
        final List<T> filteredOlds = removeDeletedItems(olds);
        Map<String, String> schemaTableIdMap = new HashMap<>();
        Map<String, T> schemaTableEntityMap = new HashMap<>();
        Map<String, Map<Long, Long>> schemaTableOptlockMap = new HashMap<>();
        List<T> itemsToUpdate = new ArrayList<>();
        for (T item : filteredOlds) {
            item.markAsUpdating(uuid);
            String schemaTable = getEntityConfig(log, item).dbTable();
            if (!schemaTableIdMap.containsKey(schemaTable)) {
                schemaTableIdMap.put(schemaTable, "" + item.getId());
                try {
                    schemaTableEntityMap.put(schemaTable, (T) getEntityConfig(log, item).newInstance());
                } catch (Throwable ex) {
                    log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + item.getClass().getSimpleName() + " CONFIGURATION"), ex);
                    throw new RuntimeException("Missing or wrong entity " + item.getClass().getSimpleName() + " configuration");
                }
                schemaTableOptlockMap.put(schemaTable, new HashMap<>());
            } else {
                schemaTableIdMap.put(schemaTable, schemaTableIdMap.get(schemaTable) + "," + item.getId());
            }
            schemaTableOptlockMap.get(schemaTable).put(item.getId(), item.getOptlock());
            if (news.containsKey(item.getId())) {
                T itemToUpdate = news.get(item.getId());
                itemToUpdate.markAsUpdateToApply(uuid, item.getId(), item.getOptlock());
                itemsToUpdate.add(itemToUpdate);
            }
        }
        if (filteredOlds.size() != itemsToUpdate.size()) {
            log.error(getTruncatedString("DB ROLLBACK " + uuid + " DUE TO "
                    + itemsToUpdate.size() + " UPDATE_TO_APPLY ROWS MISMATCH THE " + filteredOlds.size() + " UPDATING ONES"));
            throw new RuntimeException(itemsToUpdate.size() + " UPDATE_TO_APPLY rows mismatch the " + filteredOlds.size() + " UPDATING ones");
        } else {
            return executeManyMergeOrRemove(log, conn, uuid, expire, schemaTableIdMap, schemaTableEntityMap,
                    schemaTableOptlockMap, filteredOlds.size(), DatabaseEventState.UPDATING)
                    .chain((v) -> executeManyPersist(log, conn, expire, itemsToUpdate))
                    .map(newItems -> {
                        List<T> itemsChanged = new ArrayList<>();
                        itemsChanged.addAll(filteredOlds);
                        itemsChanged.addAll(newItems);
                        return itemsChanged;
                    });
        }
    }

    private <T extends IEntity> Uni<List<T>> executeGetResultList(Logger log, SqlConnection conn, String query, T responseObject,
            boolean includeDeleted, boolean mustBeOne) {
        QueryResponse auxQueryResp = new QueryResponse();
        return execute(conn, query)
                .onItem().transformToMulti(queryResp -> {
                    auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB RESULT [" + queryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    return Multi.createFrom().iterable(queryResp.getRowSet());
                })
                .onItem().transform((row) -> {
                    try {
                        try {
                            return (T) getEntityConfig(log, responseObject).from(getEntityConfig(log, responseObject).newInstance(), row);
                        } catch (Throwable ex) {
                            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + responseObject.getClass().getSimpleName() + " CONFIGURATION"), ex);
                            throw new RuntimeException("Missing or wrong entity " + responseObject.getClass().getSimpleName() + " configuration");
                        }
                    } catch (Throwable ex) {
                        log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query.toString() + "; PARAMS: []"
                                + " -> ERROR: MISSING OR WRONG ENTITY " + responseObject.getClass().getSimpleName() + " CONFIGURATION"), ex);
                        throw new RuntimeException("Missing or wrong entity " + responseObject.getClass().getSimpleName() + " configuration");
                    }
                })
                .filter(item -> includeDeleted || item.getEvent().getState() == null || !item.getEvent().getState().equals(DatabaseEventState.DELETED))
                .collect().asList().invoke(items -> {
                    if (mustBeOne) {
                        if (items.isEmpty()) {
                            log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                    + " -> ERROR: " + responseObject.getClass().getSimpleName() + " NOT FOUND"));
                            throw new RuntimeException(responseObject.getClass().getSimpleName() + " not found");
                        } else if (items.size() > 1) {
                            String itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                            log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                    + " -> ERROR: " + responseObject.getClass().getSimpleName()
                                    + " SHOULD BE JUST ONE BUT FOUND MANY ITEMS BECAUSE OF CONCURRENT ACCESS: " + itemsString));
                            T first = items.get(0);
                            throw new RuntimeException(responseObject.getClass().getSimpleName() + " with id " + first.getId()
                                    + " has concurrent access because its state is " + first.getEvent().getState());
                        } else {
                            log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                    + " -> ITEM: " + jsoner.json(log, items.get(0))));
                        }
                    } else {
                        String itemsString = null;
                        if (items != null && !items.isEmpty()) {
                            itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                        }
                        log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ITEMS: " + (itemsString != null ? itemsString : "[]")));
                    }
                });
    }

    private <T extends IEntity> Uni<List<T>> executeGetResultList(Logger log, SqlConnection conn, String query, Object[] parameters,
            T responseObject, boolean includeDeleted, boolean mustBeOne) {
        QueryParameters params = new QueryParameters(parameters);
        QueryResponse auxQueryResp = new QueryResponse();
        return execute(conn, query, params)
                .onItem().transformToMulti(queryResp -> {
                    auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB RESULT [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    return Multi.createFrom().iterable(queryResp.getRowSet());
                })
                .onItem().transform((row) -> {
                    try {
                        try {
                            return (T) getEntityConfig(log, responseObject).from(getEntityConfig(log, responseObject).newInstance(), row);
                        } catch (Throwable ex) {
                            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + responseObject.getClass().getSimpleName() + " CONFIGURATION"), ex);
                            throw new RuntimeException("Missing or wrong entity " + responseObject.getClass().getSimpleName() + " configuration");
                        }
                    } catch (Throwable ex) {
                        log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query.toString() + "; PARAMS: []"
                                + " -> ERROR: MISSING OR WRONG ENTITY " + responseObject.getClass().getSimpleName() + " CONFIGURATION"), ex);
                        throw new RuntimeException("Missing or wrong entity " + responseObject.getClass().getSimpleName() + " configuration");
                    }
                })
                .filter(item -> includeDeleted || item.getEvent().getState() == null || !item.getEvent().getState().equals(DatabaseEventState.DELETED))
                .collect().asList().invoke(items -> {
                    if (mustBeOne) {
                        if (items.isEmpty()) {
                            log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                    + " -> ERROR: " + responseObject.getClass().getSimpleName() + " NOT FOUND"));
                            throw new RuntimeException(responseObject.getClass().getSimpleName() + " not found");
                        } else if (items.size() > 1) {
                            String itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                            log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                    + " -> ERROR: " + responseObject.getClass().getSimpleName()
                                    + " SHOULD BE JUST ONE BUT FOUND MANY ITEMS BECAUSE OF CONCURRENT ACCESS: " + itemsString));
                            T first = items.get(0);
                            throw new RuntimeException(responseObject.getClass().getSimpleName() + " with id " + first.getId()
                                    + " has concurrent access because its state is " + first.getEvent().getState());
                        } else {
                            log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                    + " -> ITEM: " + jsoner.json(log, items.get(0))));
                        }
                    } else {
                        String itemsString = null;
                        if (items != null && !items.isEmpty()) {
                            itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                        }
                        log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ITEMS: " + (itemsString != null ? itemsString : "[]")));
                    }
                });
    }

    private <T extends IEntity> Uni<T> executeOnePersist(Logger log, SqlConnection conn, OffsetDateTime expire, T e) {
        EntityConfiguration entityConfig = getEntityConfig(log, e);
        List<String> columns = (List<String>) entityConfig.dbColumns().stream()
                .filter(
                        col -> (!((String) col).toLowerCase().equals("id") || e.getId() != null)
                        && !((String) col).toLowerCase().equals("optlock") && !((String) col).toLowerCase().equals("event_uuid")
                        && !((String) col).toLowerCase().equals("event_state")
                        && (entityConfig.dbColumnsExcludedFromInsert() == null || !entityConfig.dbColumnsExcludedFromInsert().contains((String) col))
                )
                .collect(Collectors.toList());
        String query = String.format("INSERT INTO %s(%soptlock, event_uuid, event_state) SELECT %s WHERE now() < %s RETURNING *",
                getEntityConfig(log, e).dbTable(), getColumnNamesList(columns), getColumnValuePlaceholdersList(columns), "$" + (columns.size() + 4));
        QueryParameters params = new QueryParameters(getColumnValuesList(log, entityConfig, columns, e, expire));
        return execute(conn, query, params)
                .onItem().transform(queryResp -> {
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB ROLLBACK [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    RowIterator<Row> it = queryResp.getRowSet().iterator();
                    if (!it.hasNext()) {
                        String timeoutExceptionMessage = "";
                        OffsetDateTime now = OffsetDateTime.now();
                        if (expire.isBefore(now)) {
                            timeoutExceptionMessage = " due timeout exception";
                        }
                        log.error(getTruncatedString("DB ROLLBACK [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: NO ITEM INSERTED" + timeoutExceptionMessage.toUpperCase()));
                        throw new RuntimeException(e.getClass().getSimpleName() + " not persisted" + timeoutExceptionMessage);
                    }
                    T entity = null;
                    try {
                        entity = (T) getEntityConfig(log, e).from(e, it.next());
                    } catch (Throwable ex) {
                        log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + e.getClass().getSimpleName() + " CONFIGURATION"), ex);
                        throw new RuntimeException("Missing or wrong entity " + e.getClass().getSimpleName() + " configuration");
                    }
                    log.info(getTruncatedString("DB COMMIT [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                            + " -> ITEM: " + (entity != null ? jsoner.json(log, entity) : null)));
                    return entity;
                });
    }

    private <T extends IEntity> Uni<List<T>> executeManyPersist(Logger log, SqlConnection conn, OffsetDateTime expire, List<T> items) {
        List<T> insertedItems = new ArrayList<>();
        Uni<Void> loopRoot = Uni.createFrom().voidItem();
        for (T item : items) {
            loopRoot = loopRoot.chain(() -> executeOnePersist(log, conn, expire, item)
                    .invoke(c -> insertedItems.add(c)).replaceWithVoid());
        }
        return loopRoot.map((v) -> insertedItems);
    }

    private <T extends IEntity> Uni<T> executeOneMergeOrRemove(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire, T e, Long requiredOptlock,
            DatabaseEventState originalState) {
        if (requiredOptlock == null) {
            requiredOptlock = e.getOptlock();
        }
        final Long finalRequiredOptlock = requiredOptlock;
        String query = String.format("UPDATE %s SET event_uuid = $1, event_state = $2 WHERE optlock = $3 AND id = $4 AND event_state IS NULL AND now() < $5 RETURNING *",
                getEntityConfig(log, e).dbTable());
        QueryParameters params = new QueryParameters(new Object[]{uuid, e.getEvent().getState(), requiredOptlock, e.getId(), expire});
        return execute(conn, query, params)
                .onItem().transform(queryResp -> {
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB ROLLBACK [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    RowIterator<Row> it = queryResp.getRowSet().iterator();
                    if (!it.hasNext()) {
                        String exceptionDetails = " for concurrent transactions";
                        OffsetDateTime now = OffsetDateTime.now();
                        if (expire.isBefore(now)) {
                            exceptionDetails = " for timeout exception";
                        }
                        log.error(getTruncatedString("DB ROLLBACK [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ITEM: NO ITEM " + (e.getEvent().getState().equals(DatabaseEventState.DELETING) ? "DELETED" : "UPDTADE")
                                + " WITH ID " + e.getId() + ", STATE \"" + originalState + "\" AND OPTLOCK " + finalRequiredOptlock
                                + exceptionDetails.toUpperCase()));
                        throw new RuntimeException(e.getClass().getSimpleName() + " with id " + e.getId() + " not set as " + e.getEvent().getState() + exceptionDetails);
                    } else if (queryResp.getRowSet().rowCount() > 1) {
                        String itemsString = "";
                        while (it.hasNext()) {
                            T entity = null;
                            try {
                                entity = (T) getEntityConfig(log, e).from(e, it.next());
                            } catch (Throwable ex) {
                                log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + e.getClass().getSimpleName() + " CONFIGURATION"), ex);
                                throw new RuntimeException("Missing or wrong entity " + e.getClass().getSimpleName() + " configuration");
                            }
                            itemsString += "," + (entity != null ? jsoner.json(log, entity) : null);
                        }
                        if (itemsString.length() != 0) {
                            itemsString = itemsString.substring(1);
                        }
                        log.error(getTruncatedString("DB ROLLBACK [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ITEMS (ERROR - SHOULD BE JUST ONE BUT WERE "
                                + (e.getEvent().getState().equals(DatabaseEventState.DELETING) ? "DELETED" : "UPDTADE")
                                + " MANY): [" + itemsString + "]"));
                        throw new RuntimeException(e.getClass().getSimpleName() + " with id " + e.getId() + " not set as " + e.getEvent().getState()
                                + " for concurrent transactions");
                    } else {
                        T entity = null;
                        try {
                            entity = (T) getEntityConfig(log, e).from(e, it.next());
                        } catch (Throwable ex) {
                            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + e.getClass().getSimpleName() + " CONFIGURATION"), ex);
                            throw new RuntimeException("Missing or wrong entity " + e.getClass().getSimpleName() + " configuration");
                        }
                        log.info(getTruncatedString("DB COMMIT [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ITEM: " + (entity != null ? jsoner.json(log, entity) : null)));
                        return entity;
                    }
                });
    }

    private <T extends IEntity> Uni<Void> executeManyMergeOrRemove(Logger log, SqlConnection conn, String uuid, OffsetDateTime expire,
            Map<String, String> schemaTableIdMap, Map<String, T> schemaTableEntityMap, Map<String, Map<Long, Long>> schemaTableOptlockMap,
            int expectedRowsChanged, DatabaseEventState state) {
        final Map<String, Integer> schemaTableRowsChanged = new HashMap<>();
        final List<String> logs = new ArrayList<>();
        QueryResponse auxQueryResp = new QueryResponse();
        Uni<Void> loopRoot = Uni.createFrom().voidItem();
        for (String schemaTable : schemaTableIdMap.keySet()) {
            String query = String.format("UPDATE %s SET event_uuid = $1, event_state = $2 WHERE id IN (%s) AND event_state IS NULL AND now() < $3 RETURNING *",
                    schemaTable, schemaTableIdMap.get(schemaTable));
            QueryParameters params = new QueryParameters(new Object[]{uuid, state, expire});
            T e = schemaTableEntityMap.get(schemaTable);
            Map<Long, Long> optlockMap = schemaTableOptlockMap.get(schemaTable);
            loopRoot = loopRoot.chain(() -> execute(conn, query, params)
                    .invoke(queryResp -> {
                        auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                        if (queryResp.getError() != null) {
                            log.error(getTruncatedString("DB ROLLBACK " + state.name() + " [" + queryResp.getExecutionTime() + "]: "
                                    + query + "; " + logParam(log, params) + " -> ERROR: " + queryResp.getError()));
                            throw new RuntimeException(queryResp.getError());
                        }
                        schemaTableRowsChanged.put(schemaTable, queryResp.getRowSet().rowCount());
                        RowIterator<Row> it = queryResp.getRowSet().iterator();
                        String itemsString = "";
                        String errors = "";
                        while (it.hasNext()) {
                            T entity = null;
                            try {
                                entity = (T) getEntityConfig(log, e).from(e, it.next());
                            } catch (Throwable ex) {
                                log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + e.getClass().getSimpleName() + " CONFIGURATION"), ex);
                                throw new RuntimeException("Missing or wrong entity " + e.getClass().getSimpleName() + " configuration");
                            }
                            if (entity == null || entity.getId() == null) {
                                errors += ", " + e.getClass().getSimpleName() + " not found during " + state.name();
                            } else {
                                Long optlockToMatch = -1L;
                                if (optlockMap.containsKey(entity.getId())) {
                                    optlockToMatch = optlockMap.get(entity.getId());
                                }
                                if (!Objects.equals(entity.getOptlock(), optlockToMatch)) {
                                    errors += ", " + entity.getClass().getSimpleName() + " with id " + entity.getId()
                                            + " not set as " + state.name() + " for optimistic locking mismatch";
                                }
                            }
                            itemsString += "," + (entity != null ? jsoner.json(log, entity) : null);
                        }
                        if (itemsString.length() != 0) {
                            itemsString = itemsString.substring(1);
                        }
                        if (errors.isEmpty()) {
                            logs.add(getTruncatedString("DB COMMIT " + state.name() + " [" + queryResp.getExecutionTime() + "]: "
                                    + query + "; " + logParam(log, params) + " -> ITEMS: [" + itemsString + "]"));
                        } else {
                            errors = errors.substring(2);
                            OffsetDateTime now = OffsetDateTime.now();
                            if (expire.isBefore(now)) {
                                errors = "timeout exception";
                            }
                            log.error(getTruncatedString("DB ROLLBACK " + state.name() + " [" + queryResp.getExecutionTime() + "]: "
                                    + query + "; " + logParam(log, params) + " -> ERROR: " + errors.toUpperCase()));
                            throw new RuntimeException(errors);
                        }
                    }).replaceWithVoid());
        }
        return loopRoot.map((v) -> {
            int totalRowsChanged = 0;
            for (int rowsChanged : schemaTableRowsChanged.values()) {
                totalRowsChanged += rowsChanged;
            }
            if (totalRowsChanged != expectedRowsChanged) {
                OffsetDateTime now = OffsetDateTime.now();
                if (expire.isBefore(now)) {
                    log.error(getTruncatedString("DB ROLLBACK [" + auxQueryResp.getExecutionTime() + "] " + uuid + " DUE TO TIMEOUT EXCEPTION"));
                    throw new RuntimeException("timeout exception");
                } else {
                    log.error(getTruncatedString("DB ROLLBACK [" + auxQueryResp.getExecutionTime() + "] " + uuid + " DUE TO UNEXPECTED ROWS "
                            + (state.equals(DatabaseEventState.DELETING) ? "DELETED" : "UPDATED")
                            + " (" + totalRowsChanged + " CHANGED VS " + expectedRowsChanged + " TO CHANGE)"));
                    throw new RuntimeException(totalRowsChanged + " "
                            + (state.equals(DatabaseEventState.DELETING) ? "deleted" : "updated")
                            + " rows mismatch the " + expectedRowsChanged
                            + " ones to " + (state.equals(DatabaseEventState.DELETING) ? "delete" : "update")
                            + "(concurrently db access probably)");
                }
            } else {
                logs.stream().forEach(l -> log.info(l));
            }
            return v;
        });
    }

    private String getColumnNamesList(List<String> columns) {
        return columns.stream().map(c -> c + ", ").reduce((c1, c2) -> c1 + c2).get();
    }

    private String getColumnValuePlaceholdersList(List<String> columns) {
        StringBuilder sb = new StringBuilder("");
        for (int i = 1; i < columns.size() + 4; i++) {
            sb.append(" ,$").append(i);
        }
        return sb.toString().substring(2);
    }

    private <T extends IEntity> Object[] getColumnValuesList(Logger log, EntityConfiguration entityConfig,
            List<String> columns, T e, OffsetDateTime expire) {
        List<Object> params = new ArrayList<>();
        try {
            for (String c : columns) {
                Map<String, Object> parameters = entityConfig.params(e);
                if (parameters.containsKey(c)) {
                    params.add(parameters.get(c));
                }
            }
        } catch (Throwable ex) {
            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + e.getClass().getSimpleName() + " CONFIGURATION"), ex);
            throw new RuntimeException("Missing or wrong entity " + e.getClass().getSimpleName() + " configuration");
        }
        params.add(e.getOptlock());
        params.add(e.getEvent().getUuid());
        params.add(e.getEvent().getState());
        params.add(expire);
        return params.toArray(new Object[]{});
    }

    private <T extends IEntity> void check(Logger log, String uuid, T e, Long requiredOptlock, boolean isForDeleting) {
        if (e == null) {
            log.error(getTruncatedString("DB CHECK " + uuid + ": ENTITY NOT FOUND"));
            throw new RuntimeException("Entity not found");
        } else if (e.getEvent().getState() != null) {
            if (e.getEvent().getState().equals(DatabaseEventState.DELETED)) {
                log.error(getTruncatedString("DB CHECK " + uuid + ": " + e.getClass().getSimpleName() + " WITH ID " + e.getId()
                        + (isForDeleting ? " WAS ALREADAY \"DELETED\"" : "IS \"DELETED\"")));
                throw new RuntimeException(e.getClass().getSimpleName() + " with id " + e.getId()
                        + (isForDeleting ? " was already DELETED" : " is DELETED"));
            } else {
                log.error(getTruncatedString("DB CHECK " + uuid + ": " + e.getClass().getSimpleName() + " WITH ID " + e.getId()
                        + " HAS CONCURRENT ACCESS BECAUSE ITS STATE IS \"" + e.getEvent().getState() + "\""));
                throw new RuntimeException(e.getClass().getSimpleName() + " with id " + e.getId()
                        + " has concurrent access because its state is " + e.getEvent().getState());
            }
        } else if (requiredOptlock != null && !Objects.equals(e.getOptlock(), requiredOptlock)) {
            log.error(getTruncatedString("DB CHECK " + uuid + ": " + e.getClass().getSimpleName() + " WITH ID " + e.getId()
                    + " HAS OPTIMISTIC LOCKING MISMATCH, " + requiredOptlock + " EXPECTED BUT " + e.getOptlock() + " FOUND"));
            throw new RuntimeException(e.getClass().getSimpleName() + " with id " + e.getId()
                    + " has optimistic locking mismatch, " + requiredOptlock + " expected but " + e.getOptlock() + " found");
        }
    }

    private <T extends IEntity> void check(Logger log, String uuid, List<T> items) {
        if (items == null) {
            log.error(getTruncatedString("DB CHECK " + uuid + ": ENTITY LIST CAN NOT BE NULL"));
            throw new RuntimeException("Entity list can not be null");
        } else if (items.stream()
                .anyMatch(item -> item.getEvent().getState() != null && !item.getEvent().getState().equals(DatabaseEventState.DELETED))) {
            StringBuffer sb = new StringBuffer();
            items.stream()
                    .filter(item -> item.getEvent().getState() != null && !item.getEvent().getState().equals(DatabaseEventState.DELETED))
                    .forEach(item -> sb.append("; ").append(item.getClass().getSimpleName()).append(" with id ").append(item.getId())
                    .append(" is ").append(item.getEvent().getState().name()));
            String error = sb.toString().substring(2);
            log.error(getTruncatedString("DB CHECK " + uuid + ": " + error));
            throw new RuntimeException(error);
        }
    }

    private <T extends IEntity> List<T> removeDeletedItems(List<T> items) {
        return items.stream().filter(item -> item.getEvent().getState() == null
                || !item.getEvent().getState().equals(DatabaseEventState.DELETED)).collect(Collectors.toList());
    }

    private <T extends IEntity> EntityConfiguration getEntityConfig(Logger log, T item) {
        try {
            return entitiesConfigurator.get((Class<IEntity>) item.getClass());
        } catch (Throwable ex) {
            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + item.getClass().getSimpleName() + " CONFIGURATION"), ex);
            throw new RuntimeException("Missing or wrong entity " + item.getClass().getSimpleName() + " configuration");
        }
    }

    private String logParam(Logger log, QueryParameters parameters) {
        if (parameters == null) {
            return "";
        }
        return jsoner.json(log, parameters).replaceFirst("\\{\"params\":", "PARAMS: {");
    }
}
