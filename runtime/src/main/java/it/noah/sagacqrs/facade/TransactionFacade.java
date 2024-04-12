/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.facade;

import io.smallrye.mutiny.Uni;
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

    public Uni<Transaction> create(String uuid, String operation, long timeout) {
        return dao.create(uuid, operation, timeout);
    }

    public Uni<Transaction> commit(String uuid) {
        return dao.commit(uuid);
    }

    public Uni<Transaction> rollback(String uuid) {
        return dao.rollback(uuid);
    }

    public Uni<List<Transaction>> finalizeCommitAndRollback() {
        return dao.finalizeCommitAndRollback();
    }

    public Uni<List<Transaction>> delete(List<Transaction> transactions) {
        return dao.delete(transactions);
    }

    public Uni<List<Transaction>> fixPendingAndFinalizeExpired() {
        return dao.fixPendingAndFinalizeExpired();
    }
}
