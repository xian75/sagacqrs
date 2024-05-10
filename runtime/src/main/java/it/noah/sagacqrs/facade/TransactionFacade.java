/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.facade;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import it.noah.sagacqrs.dao.TransactionDao;
import it.noah.sagacqrs.entity.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/**
 *
 * @author NATCRI
 */
@ApplicationScoped
public class TransactionFacade {

    @Inject
    TransactionDao dao;

    public Uni<Transaction> create(Pool dbPool, String uuid, String operation, long timeout) {
        return dao.create(dbPool, uuid, operation, timeout);
    }

    public Uni<Transaction> commit(Pool dbPool, String uuid) {
        return dao.commit(dbPool, uuid);
    }

    public Uni<Transaction> rollback(Pool dbPool, String uuid) {
        return dao.rollback(dbPool, uuid);
    }

    public Uni<List<Transaction>> finalizeCommitAndRollback(Pool dbPool) {
        return dao.finalizeCommitAndRollback(dbPool);
    }

    public Uni<List<Transaction>> delete(Pool dbPool, List<Transaction> transactions) {
        return dao.delete(dbPool, transactions);
    }

    public Uni<List<Transaction>> fixPendingAndFinalizeExpired(Pool dbPool) {
        return dao.fixPendingAndFinalizeExpired(dbPool);
    }
}
