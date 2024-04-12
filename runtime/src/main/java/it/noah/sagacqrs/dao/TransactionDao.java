/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.dao;

import it.noah.sagacqrs.dao.interfaces.ICommonDao;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import it.noah.sagacqrs.enums.TransactionOutcome;
import it.noah.sagacqrs.dao.dto.QueryParameters;
import it.noah.sagacqrs.dao.dto.QueryResponse;
import it.noah.sagacqrs.entity.Transaction;
import it.noah.sagacqrs.json.Jsoner;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

/**
 *
 * @author NATCRI
 */
@ApplicationScoped
public class TransactionDao implements ICommonDao {

    @Inject
    Logger log;

    @Inject
    PgPool dbPool;

    @Inject
    Jsoner jsoner;

    public Uni<Transaction> create(String uuid, String operation, long timeout) {
        Transaction t = new Transaction();
        t.setUuid(uuid);
        t.setOperation(operation);
        t.setTimeout(timeout);
        t.setOutcome(TransactionOutcome.PENDING.name());
        OffsetDateTime expire = OffsetDateTime.now();
        expire = expire.plus(timeout, ChronoUnit.MILLIS);
        t.setExpire(expire);
        String query = "INSERT INTO orchestrator.transactions(event_uuid, operation, outcome, expire, timeout)"
                + " VALUES($1, $2, $3, $4, $5) RETURNING *";
        QueryParameters params = new QueryParameters(t.array());
        return dbPool.withTransaction(conn -> execute(conn, query, params))
                .onItem().transform(queryResp -> {
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB TRANSACTION ERROR [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    RowIterator<Row> it = queryResp.getRowSet().iterator();
                    if (!it.hasNext()) {
                        log.error(getTruncatedString("DB TRANSACTION ERROR [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: NO TRANSACTION INSERTED"));
                        throw new RuntimeException(t.getClass().getSimpleName() + " not persisted");
                    }
                    Transaction transaction = t.from(it.next());
                    log.info(getTruncatedString("DB TRANSACTION SUCCESS [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                            + " -> TRANSACTION INSERTED: " + (transaction != null ? jsoner.json(log, transaction) : null)));
                    return transaction;
                });
    }

    public Uni<Transaction> commit(String uuid) {
        return commitOrRollback(uuid, TransactionOutcome.COMMIT);
    }

    public Uni<Transaction> rollback(String uuid) {
        return commitOrRollback(uuid, TransactionOutcome.ROLLBACK);
    }

    private Uni<Transaction> commitOrRollback(String uuid, TransactionOutcome outcome) {
        Transaction t = new Transaction();
        t.setUuid(uuid);
        t.setOutcome(outcome.name());
        String query = "UPDATE orchestrator.transactions SET outcome = $1 WHERE event_uuid = $2 AND outcome = 'PENDING' RETURNING *";
        QueryParameters params = new QueryParameters(new Object[]{outcome.name(), uuid});
        return dbPool.withTransaction(conn -> execute(conn, query, params))
                .onItem().transform(queryResp -> {
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB TRANSACTION ERROR [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    RowIterator<Row> it = queryResp.getRowSet().iterator();
                    if (!it.hasNext()) {
                        log.error(getTruncatedString("DB TRANSACTION ERROR [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                                + " -> ERROR: NO TRANSACTION SET TO " + outcome.name()));
                        throw new RuntimeException(t.getClass().getSimpleName() + " not persisted");
                    }
                    Transaction transaction = t.from(it.next());
                    log.info(getTruncatedString("DB TRANSACTION SUCCESS [" + queryResp.getExecutionTime() + "]: " + query + "; " + logParam(log, params)
                            + " -> TRANSACTION SET TO " + outcome.name() + ": " + (transaction != null ? jsoner.json(log, transaction) : null)));
                    return transaction;
                });
    }

    public Uni<List<Transaction>> finalizeCommitAndRollback() {
        QueryResponse auxQueryResp = new QueryResponse();
        String query = "UPDATE orchestrator.transactions SET outcome = 'FINALIZE_' || outcome WHERE outcome IN ('COMMIT','ROLLBACK') RETURNING *";
        return dbPool.withTransaction(conn -> execute(conn, query))
                .onItem().transformToMulti(queryResp -> {
                    auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB RESULT [" + queryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    return Multi.createFrom().iterable(queryResp.getRowSet());
                })
                .onItem().transform((row) -> (new Transaction()).fromToFinalize(row))
                .collect().asList().invoke(items -> {
                    if (!items.isEmpty()) {
                        String itemsString = null;
                        if (items != null && !items.isEmpty()) {
                            itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                        }
                        log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ITEMS: " + (itemsString != null ? itemsString : "[]")));
                    }
                });
    }

    public Uni<List<Transaction>> delete(List<Transaction> transactions) {
        if (transactions == null || transactions.isEmpty()) {
            return Uni.createFrom().item(new ArrayList<>());
        }
        QueryResponse auxQueryResp = new QueryResponse();
        String query = String.format("DELETE FROM orchestrator.transactions WHERE event_uuid IN (%s) RETURNING *",
                getListOfStringAsString(transactions.stream().map(t -> t.getUuid()).collect(Collectors.toList())));
        return dbPool.withTransaction(conn -> execute(conn, query))
                .onItem().transformToMulti(queryResp -> {
                    auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB RESULT [" + queryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    return Multi.createFrom().iterable(queryResp.getRowSet());
                })
                .onItem().transform((row) -> (new Transaction()).fromToFinalize(row))
                .collect().asList().invoke(items -> {
                    String itemsString = null;
                    if (items != null && !items.isEmpty()) {
                        itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                    }
                    log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                            + " -> ITEMS: " + (itemsString != null ? itemsString : "[]")));
                });
    }

    public Uni<List<Transaction>> fixPendingAndFinalizeExpired() {
        QueryResponse auxQueryResp = new QueryResponse();
        String query = "UPDATE orchestrator.transactions SET"
                + " outcome = CASE WHEN outcome = 'PENDING' THEN 'FINALIZE_ROLLBACK' ELSE outcome END,"
                + " expire = NOW() + interval '1 milliseconds' * timeout"
                + " WHERE expire < NOW() AND outcome IN ('PENDING','FINALIZE_COMMIT','FINALIZE_ROLLBACK') RETURNING *";
        return dbPool.withTransaction(conn -> execute(conn, query))
                .onItem().transformToMulti(queryResp -> {
                    auxQueryResp.setExecutionTime(queryResp.getExecutionTime());
                    if (queryResp.getError() != null) {
                        log.error(getTruncatedString("DB RESULT [" + queryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ERROR: " + queryResp.getError()));
                        throw new RuntimeException(queryResp.getError());
                    }
                    return Multi.createFrom().iterable(queryResp.getRowSet());
                })
                .onItem().transform((row) -> (new Transaction()).fromToFinalize(row))
                .collect().asList().invoke(items -> {
                    if (!items.isEmpty()) {
                        String itemsString = null;
                        if (items != null && !items.isEmpty()) {
                            itemsString = items.stream().map(i -> jsoner.json(log, i)).collect(Collectors.toList()).toString();
                        }
                        log.info(getTruncatedString("DB RESULT [" + auxQueryResp.getExecutionTime() + "]: " + query + "; PARAMS: []"
                                + " -> ITEMS: " + (itemsString != null ? itemsString : "[]")));
                    }
                });
    }

    private String logParam(Logger log, QueryParameters parameters) {
        if (parameters == null) {
            return "";
        }
        return jsoner.json(log, parameters).replaceFirst("\\{\"params\":", "PARAMS: {");
    }
}