/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.participant;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.SqlConnection;
import it.noah.sagacqrs.dao.dto.QueryResponse;
import it.noah.sagacqrs.dao.interfaces.ICommonDao;
import it.noah.sagacqrs.entity.Transaction;
import it.noah.sagacqrs.entity.interfaces.IEntity;
import it.noah.sagacqrs.json.Jsoner;
import it.noah.sagacqrs.participant.dto.EntityConfiguration;
import it.noah.sagacqrs.participant.dto.ParticipantConfiguration;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 *
 * @author NATCRI
 */
@Dependent
public class ParticipantConfigurator implements ICommonDao {

    private ParticipantConfiguration configuration;

    @Inject
    private EntitiesConfigurator entitiesConfigurator;

    @Inject
    Jsoner jsoner;

    public void init(Logger log, PgPool dbPool, Class... classes) throws Throwable {
        List<IEntity> entities = new ArrayList<>();
        for (Class clazz : classes) {
            entities.add((IEntity) clazz.getDeclaredConstructor().newInstance());
        }
        configuration = new ParticipantConfiguration(log, dbPool, entities);
        entitiesConfigurator.add(classes);
    }

    public Uni<Response> finalizeOperations(List<Transaction> transactions) {
        return finalizeCommitAndRollback(configuration.getLog(), configuration.getDbPool(), configuration.getTables(), transactions)
                .onItemOrFailure().transform((result, failure) -> {
                    if (failure != null) {
                        configuration.getLog().debug("PARTICIPANT RESPONSE ERROR: " + failure.getMessage());
                        return Response.accepted(failure.getMessage()).build();
                    } else {
                        return Response.ok(result).build();
                    }
                });
    }

    private Uni<Integer> finalizeCommitAndRollback(Logger log, PgPool dbPool, List<IEntity> tables, List<Transaction> transactions) {
        List<String> transactionsToCommit = transactions.stream().filter(t -> t.getOutcome().equals("FINALIZE_COMMIT"))
                .map(t -> t.getUuid()).collect(Collectors.toList());
        List<String> transactionsToRollback = transactions.stream().filter(t -> t.getOutcome().equals("FINALIZE_ROLLBACK"))
                .map(t -> t.getUuid()).collect(Collectors.toList());
        return finalizeCommitAndRollback(log, dbPool, tables, transactionsToCommit, transactionsToRollback);
    }

    private Uni<Integer> finalizeCommitAndRollback(Logger log, PgPool dbPool, List<IEntity> tables,
            List<String> uuidsToCommit, List<String> uuidsToRollback) {
        final Integer[] acc = new Integer[1];
        return dbPool.withTransaction(conn -> commit(log, conn, uuidsToCommit, tables)
                .chain(c -> {
                    acc[0] = c;
                    return rollback(log, conn, uuidsToRollback, tables);
                })
                .map(r -> acc[0] + r)
        );
    }

    private Uni<Integer> commit(Logger log, SqlConnection conn, List<String> uuids, List<IEntity> tables) {
        return updateAllTables(log, conn, uuids, tables, true);
    }

    private Uni<Integer> rollback(Logger log, SqlConnection conn, List<String> uuids, List<IEntity> tables) {
        return updateAllTables(log, conn, uuids, tables, false);
    }

    private Uni<Integer> updateAllTables(Logger log, SqlConnection conn, List<String> uuids, List<IEntity> tables, boolean isCommit) {
        if (uuids == null || uuids.isEmpty()) {
            return Uni.createFrom().item(0);
        }
        final List<Integer> affectedRowsCount = new ArrayList<>();
        Uni<Void> loopRoot = Uni.createFrom().voidItem();
        for (IEntity table : tables) {
            loopRoot = applyLoopRoot(loopRoot, affectedRowsCount, log, conn, uuids, table, isCommit);
        }
        return loopRoot.map(v -> affectedRowsCount.stream().mapToInt(Integer::intValue).sum());
    }

    private Uni<Void> applyLoopRoot(Uni<Void> loopRoot, List<Integer> affectedRowsCount, Logger log, SqlConnection conn,
            List<String> uuids, IEntity table, boolean isCommit) {
        return loopRoot.chain(() -> updateOneTable(log, conn, uuids, table, isCommit)
                .invoke(word -> affectedRowsCount.add(word)).replaceWithVoid());
    }

    private Uni<Integer> updateOneTable(Logger log, SqlConnection conn, List<String> uuids, IEntity table, boolean isCommit) {
        String dbTableName = null;
        try {
            dbTableName = getEntityConfig(log, table).dbTable();
        } catch (Throwable ex) {
            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + table.getClass().getSimpleName() + " CONFIGURATION"));
            throw new RuntimeException("Missing or wrong entity " + table.getClass().getSimpleName() + " configuration");
        }
        final StringBuilder query = new StringBuilder();
        if (isCommit) {
            query.append(String.format("UPDATE %s SET event_state = CASE WHEN event_state IN (0,3) THEN null ELSE 4 END WHERE event_state IS NOT NULL AND event_uuid IN (%s) RETURNING *",
                    dbTableName, getListOfStringAsString(uuids)));
        } else {
            query.append(String.format("UPDATE %s SET event_state = CASE WHEN event_state IN (1,2) THEN null ELSE 4 END WHERE event_state IS NOT NULL AND event_uuid IN (%s) RETURNING *",
                    dbTableName, getListOfStringAsString(uuids)));
        }
        QueryResponse auxQueryResp = new QueryResponse();
        return execute(conn, query.toString())
                .onItem().transformToMulti(queryResp -> {
                    auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB RESULT [" + queryResp.getExecutionTime() + "]: " + query.toString() + "; PARAMS: []"
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    return Multi.createFrom().iterable(queryResp.getRowSet());
                })
                .onItem().transform((row) -> {
                    try {
                        try {
                            return getEntityConfig(log, table).from(getEntityConfig(log, table).newInstance(), row);
                        } catch (Throwable ex) {
                            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + table.getClass().getSimpleName() + " CONFIGURATION"), ex);
                            throw new RuntimeException("Missing or wrong entity " + table.getClass().getSimpleName() + " configuration");
                        }
                    } catch (Throwable ex) {
                        log.error(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query.toString() + "; PARAMS: []"
                                + " -> ERROR: MISSING OR WRONG ENTITY " + table.getClass().getSimpleName() + " CONFIGURATION"));
                        throw new RuntimeException("Missing or wrong entity " + table.getClass().getSimpleName() + " configuration");
                    }
                })
                .collect().asList().map(items -> {
                    String itemsString = null;
                    if (items != null && !items.isEmpty()) {
                        itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                    }
                    log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query.toString() + "; PARAMS: []"
                            + " -> ITEMS: " + (itemsString != null ? itemsString : "[]")));
                    return items.size();
                });
    }

    private <T extends IEntity> EntityConfiguration getEntityConfig(Logger log, T item) {
        try {
            return entitiesConfigurator.get((Class<IEntity>) item.getClass());
        } catch (Throwable ex) {
            log.error(getTruncatedString("DB CHECK -> ERROR: MISSING OR WRONG ENTITY " + item.getClass().getSimpleName() + " CONFIGURATION"), ex);
            throw new RuntimeException("Missing or wrong entity " + item.getClass().getSimpleName() + " configuration");
        }
    }

}
