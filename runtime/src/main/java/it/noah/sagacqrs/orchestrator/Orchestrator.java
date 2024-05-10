/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.orchestrator;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import it.noah.sagacqrs.dao.dto.ErrorMap;
import it.noah.sagacqrs.dao.dto.EventMessage;
import it.noah.sagacqrs.dao.dto.Participant;
import it.noah.sagacqrs.json.Jsoner;
import it.noah.sagacqrs.entity.Transaction;
import it.noah.sagacqrs.facade.TransactionFacade;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 *
 * @author NATCRI
 */
@ApplicationScoped
public class Orchestrator {

    private Logger log;
    private Pool dbPool;
    private Participant[] allParticipants;

    @Inject
    TransactionFacade transactionFacade;

    @Inject
    Jsoner jsoner;

    public void init(Logger log, Pool dbPool, Participant... participants) {
        this.log = log;
        this.dbPool = dbPool;
        allParticipants = participants;
    }

    @Scheduled(every = "{saga.scheduler.finalize}")
    void finalizeOrFix() {
        finalizeOrFixTransactions(log, allParticipants, transactionFacade.finalizeCommitAndRollback(dbPool)).subscribeAsCompletionStage();
    }

    @Scheduled(every = "{saga.scheduler.fix}")
    void fix() {
        finalizeOrFixTransactions(log, allParticipants, transactionFacade.fixPendingAndFinalizeExpired(dbPool)).subscribeAsCompletionStage();
    }

    public Uni<Response> saga(String operation, Object data, Participant... sagaParticipants) {
        EventMessage eventMessage = new EventMessage();
        try {
            final Participant[] participants = getParticipants(sagaParticipants);
            eventMessage.setOperation(operation);
            eventMessage.setExpire(OffsetDateTime.now());
            long totalTimeout = 0;
            for (int i = 0; i < participants.length; i++) {
                totalTimeout += participants[i].getSagaTimeout();
            }
            Uni<Void> loopRoot = Uni.createFrom().voidItem();
            final long ttl = totalTimeout;
            final Response[] lastResponse = new Response[1];
            for (int i = 0; i < participants.length; i++) {
                final int index = i;
                loopRoot = loopRoot.chain(() -> startSagaParticipantExecute(log, eventMessage, ttl, data, index,
                        participants, lastResponse).replaceWithVoid());
            }
            return loopRoot
                    .chain(() -> {
                        if (eventMessage.getErrors().isEmpty() && lastResponse[0] != null) {
                            try {
                                Object details = lastResponse[0].readEntity(data.getClass());
                                eventMessage.setDetails(details);
                                log.debug(getTruncatedString(jsoner.json(log, eventMessage)));
                            } catch (Exception ex) {
                                log.error(ex);
                                eventMessage.getErrors().put(participants[participants.length - 1].getName(), ex.getMessage());
                            }
                        }
                        if (eventMessage.getErrors().isEmpty()) {
                            return transactionFacade.commit(dbPool, eventMessage.getUuid());
                        } else {
                            return transactionFacade.rollback(dbPool, eventMessage.getUuid());
                        }
                    })
                    .onItemOrFailure().transform((result, failure) -> {
                        if (failure != null) {
                            log.error(getTruncatedString("ORCHESTRATOR: " + failure));
                            eventMessage.getErrors().put("ORCHESTRATOR", failure.getMessage());
                        }
                        eventMessage.setOutcome(!eventMessage.getErrors().isEmpty() ? "ERR" : "OK");
                        log.info(getTruncatedString("RESPONSE: " + jsoner.json(log, eventMessage)));
                        eventMessage.setExpire(null);
                        eventMessage.setOperation(null);
                        if (eventMessage.getErrors().isEmpty()) {
                            eventMessage.setErrors(null);
                        }
                        return Response.ok(eventMessage).build();
                    });
        } catch (Exception ex) {
            log.error(getTruncatedString("ORCHESTRATOR: " + ex.getMessage().toUpperCase()));
            eventMessage.getErrors().put("ORCHESTRATOR", ex.getMessage());
            eventMessage.setOutcome("ERR");
            log.info(getTruncatedString("RESPONSE: " + jsoner.json(log, eventMessage)));
            return Uni.createFrom().item(Response.ok(eventMessage).build());
        }
    }

    private Participant[] getParticipants(Participant... participants) throws Exception {
        if (participants == null || participants.length == 0) {
            if (allParticipants == null || allParticipants.length == 0) {
                throw new Exception("No particiants set");
            }
            return allParticipants;
        } else {
            return participants;
        }
    }

    private Uni<Void> startSagaParticipantExecute(Logger log, EventMessage eventMessage, long totalTimeout, Object data, int index,
            Participant[] participants, Response[] lastResponse) {
        if (index == 0) {
            return startSagaFirstParticipantExecute(log, eventMessage, totalTimeout, data, participants[index], lastResponse);
        } else {
            return startSagaNextParticipantExecute(log, eventMessage, data, participants[index - 1].getName(),
                    participants[index], lastResponse);
        }
    }

    private Uni<Void> startSagaFirstParticipantExecute(Logger log, EventMessage eventMessage, long totalTimeout, Object data,
            Participant participant, Response[] lastResponse) {
        return transactionFacade.create(dbPool, eventMessage.getUuid(), eventMessage.getOperation(), totalTimeout)
                .onItemOrFailure().transform((transaction, failure)
                        -> checkTransactionInsertError(log, transaction, failure, eventMessage.getErrors()))
                .chain(trans -> {
                    if (trans != null) {
                        log.debug(getTruncatedString(jsoner.json(log, trans)));
                        Response response = Response.ok(data).build();
                        return startSagaParticipantCall(log, eventMessage, data, participant, response);
                    } else {
                        return Uni.createFrom().item(Response.ok().build());
                    }
                })
                .onItemOrFailure().transform((response, failure)
                        -> {
                    lastResponse[0] = checkParticipantResponseError(log, participant.getName(),
                            response, failure, eventMessage.getErrors());
                    return lastResponse[0];
                }).replaceWithVoid();
    }

    private Uni<Void> startSagaNextParticipantExecute(Logger log, EventMessage eventMessage, Object data, String previousParticipantName,
            Participant participant, Response[] lastResponse) {
        return Uni.createFrom().item(lastResponse[0])
                .chain(response -> {
                    if (eventMessage.getErrors().isEmpty() && response != null) {
                        try {
                            return startSagaParticipantCall(log, eventMessage, data, participant, response);
                        } catch (Exception ex) {
                            log.error(previousParticipantName, ex);
                            eventMessage.getErrors().put(previousParticipantName, ex.getMessage());
                            return Uni.createFrom().item(Response.ok().build());
                        }
                    } else {
                        return Uni.createFrom().item(Response.ok().build());
                    }
                })
                .onItemOrFailure().transform((response, failure)
                        -> {
                    lastResponse[0] = checkParticipantResponseError(log, participant.getName(),
                            response, failure, eventMessage.getErrors());
                    return lastResponse[0];
                }).replaceWithVoid();
    }

    private Uni<Response> startSagaParticipantCall(Logger log, EventMessage eventMessage, Object data, Participant participant, Response response) {
        eventMessage.setExpire(eventMessage.getExpire().plus(participant.getSagaTimeout(), ChronoUnit.MILLIS));
        Object details = response.readEntity(data.getClass());
        eventMessage.setDetails(details);
        log.debug(getTruncatedString(jsoner.json(log, eventMessage)));
        return participant.getClient().execute(eventMessage.getUuid(), eventMessage.getExpire(), eventMessage.getOperation(), details);
    }

    private Uni<String> finalizeOrFixTransactions(Logger log, Participant[] participants, Uni<List<Transaction>> transactionsToFinalize) {
        if (log == null) {
            System.out.println(getTruncatedString("ORCHESTRATOR: NO LOG SET"));
            return Uni.createFrom().item("");
        }
        if (participants == null || participants.length == 0) {
            log.error(getTruncatedString("ORCHESTRATOR: NO PARTICIPANTS SET"));
            return Uni.createFrom().item("");
        }
        String uuid = UUID.randomUUID().toString();
        ErrorMap errors = new ErrorMap();
        final List<Transaction> transactions = new ArrayList<>();
        Uni<Void> loopRoot = Uni.createFrom().voidItem();
        final Response[] lastResponse = new Response[1];
        for (int i = 0; i < participants.length; i++) {
            final int index = i;
            loopRoot = loopRoot.chain(() -> finalizeParticipantExecute(log, participants, transactionsToFinalize,
                    index, transactions, errors, lastResponse).replaceWithVoid());
        }
        return loopRoot
                .chain(() -> {
                    if (errors.isEmpty() && lastResponse[0] != null && !transactions.isEmpty()) {
                        Integer total = lastResponse[0].readEntity(Integer.class);
                        log.info(getTruncatedString(participants[participants.length - 1].getName() + ": " + total + " ENTITIES FINALIZED"));
                    }
                    if (errors.isEmpty() && !transactions.isEmpty()) {
                        return transactionFacade.delete(dbPool, transactions);
                    } else {
                        return Uni.createFrom().item(Response.ok().build());
                    }
                })
                .onItemOrFailure().transform((result, failure) -> {
                    if (failure != null) {
                        log.error(getTruncatedString("ORCHESTRATOR: " + failure));
                        errors.put("ORCHESTRATOR", failure.getMessage());
                    }
                    String response = null;
                    if (!errors.isEmpty()) {
                        response = "{\"uuid\": \"" + uuid + "\", \"outcome\":\"ERR\", \"error\": \"" + errors.toString() + "\"}";
                    } else {
                        response = "{\"uuid\": \"" + uuid + "\", \"outcome\":\"OK\"}";
                    }
                    if (!transactions.isEmpty()) {
                        log.info(getTruncatedString(response));
                    }
                    return response;
                });
    }

    private Uni<Void> finalizeParticipantExecute(Logger log, Participant[] participants, Uni<List<Transaction>> transactionsToFinalize,
            int index, List<Transaction> transactions, ErrorMap errors, Response[] lastResponse) {
        if (index == 0) {
            return finalizeFirstParticipantExecute(log, participants[index], transactionsToFinalize,
                    transactions, errors, lastResponse);
        } else {
            return finalizeNextParticipantExecute(log, participants[index - 1].getName(), participants[index],
                    transactions, errors, lastResponse);
        }
    }

    private Uni<Void> finalizeFirstParticipantExecute(Logger log, Participant participant, Uni<List<Transaction>> transactionsToFinalize,
            List<Transaction> transactions, ErrorMap errors, Response[] lastResponse) {
        return transactionsToFinalize
                .onItemOrFailure().transform((result, failure) -> {
                    if (failure != null) {
                        log.error(getTruncatedString("ORCHESTRATOR: " + failure));
                        errors.put("ORCHESTRATOR", failure.getMessage());
                    }
                    return result;
                })
                .chain(trans -> {
                    if (trans != null) {
                        transactions.addAll(trans);
                        if (transactions.isEmpty()) {
                            return Uni.createFrom().item(Response.ok().build());
                        }
                        log.info(getTruncatedString("ORCHESTRATOR: " + transactions.size() + " TRANSACTIONS TO FINALIZE"));
                        return participant.getClient().finalizeCommitAndRollback(transactions);
                    } else {
                        return Uni.createFrom().item(Response.ok().build());
                    }
                })
                .onItemOrFailure().transform((response, failure)
                        -> {
                    lastResponse[0] = checkParticipantResponseError(log, participant.getName(), response, failure, errors);
                    return lastResponse[0];
                }).replaceWithVoid();
    }

    private Uni<Void> finalizeNextParticipantExecute(Logger log, String previousParticipantName, Participant participant,
            List<Transaction> transactions, ErrorMap errors, Response[] lastResponse) {
        return Uni.createFrom().item(lastResponse[0])
                .chain(respA -> {
                    if (errors.isEmpty() && respA != null && !transactions.isEmpty()) {
                        Integer total = respA.readEntity(Integer.class);
                        log.info(getTruncatedString(previousParticipantName + ": " + total + " ENTITIES FINALIZED"));
                        return participant.getClient().finalizeCommitAndRollback(transactions);
                    } else {
                        return Uni.createFrom().item(Response.ok().build());
                    }
                })
                .onItemOrFailure().transform((response, failure)
                        -> {
                    lastResponse[0] = checkParticipantResponseError(log, participant.getName(), response, failure, errors);
                    return lastResponse[0];
                }).replaceWithVoid();
    }

    private Transaction checkTransactionInsertError(Logger log, Transaction transaction, Throwable failure, Map<String, String> errors) {
        if (failure != null) {
            log.error(getTruncatedString("ORCHESTRATOR: " + failure));
            errors.put("ORCHESTRATOR", failure.getMessage());
        }
        return transaction;
    }

    private Response checkParticipantResponseError(Logger log, String participant, Response response, Throwable failure, Map<String, String> errors) {
        if (failure != null) {
            log.error(getTruncatedString(participant + ": " + failure));
            errors.put(participant, failure.getMessage());
        } else if (response != null && response.getStatus() == Response.Status.ACCEPTED.getStatusCode()) {
            String error = response.readEntity(String.class);
            log.error(getTruncatedString(participant + ": " + error));
            errors.put(participant, error);
        }
        return response;
    }

    private String getTruncatedString(String text) {
        if (text != null && text.length() > getStringDetailsMaxLength()) {
            return text.substring(0, getStringDetailsMaxLength());
        } else {
            return text;
        }
    }

    private int getStringDetailsMaxLength() {
        return getConfigProperty("sagacqrs.log.maxlength", 1024);
    }

    private int getConfigProperty(String property, int defaultValue) {
        try {
            return ConfigProvider.getConfig().getValue(property, Integer.class);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    public Uni<Response> cqrs(String operation, Object data, Participant... cqrsParticipants) {
        EventMessage eventMessage = new EventMessage();
        try {
            final Participant[] participants = getParticipants(cqrsParticipants);
            eventMessage.setOperation(operation);
            eventMessage.setExpire(OffsetDateTime.now());
            Uni<Void> loopRoot = Uni.createFrom().voidItem();
            final Response[] lastResponse = new Response[1];
            for (int i = 0; i < participants.length; i++) {
                final int index = i;
                loopRoot = loopRoot.chain(() -> startCqrsParticipantExecute(log, eventMessage, data, index,
                        participants, lastResponse).replaceWithVoid());
            }
            return loopRoot
                    .chain(() -> {
                        if (eventMessage.getErrors().isEmpty() && lastResponse[0] != null) {
                            try {
                                Object details = lastResponse[0].readEntity(data.getClass());
                                eventMessage.setDetails(details);
                                log.debug(getTruncatedString(jsoner.json(log, eventMessage)));
                            } catch (Exception ex) {
                                log.error(ex);
                                eventMessage.getErrors().put(participants[participants.length - 1].getName(), ex.getMessage());
                            }
                        }
                        return Uni.createFrom().voidItem();
                    })
                    .onItem().transform(result -> {
                        eventMessage.setOutcome(!eventMessage.getErrors().isEmpty() ? "ERR" : "OK");
                        log.info(getTruncatedString("RESPONSE: " + jsoner.json(log, eventMessage)));
                        eventMessage.setExpire(null);
                        eventMessage.setOperation(null);
                        if (eventMessage.getErrors().isEmpty()) {
                            eventMessage.setErrors(null);
                        }
                        return Response.ok(eventMessage).build();
                    });
        } catch (Exception ex) {
            log.error(getTruncatedString("ORCHESTRATOR: " + ex.getMessage().toUpperCase()));
            eventMessage.getErrors().put("ORCHESTRATOR", ex.getMessage());
            eventMessage.setOutcome("ERR");
            log.info(getTruncatedString("RESPONSE: " + jsoner.json(log, eventMessage)));
            return Uni.createFrom().item(Response.ok(eventMessage).build());
        }
    }

    private Uni<Void> startCqrsParticipantExecute(Logger log, EventMessage eventMessage, Object data, int index,
            Participant[] participants, Response[] lastResponse) {
        if (index == 0) {
            return startCqrsFirstParticipantExecute(log, eventMessage, data, participants[index], lastResponse);
        } else {
            return startCqrsNextParticipantExecute(log, eventMessage, data, participants[index - 1].getName(),
                    participants[index], lastResponse);
        }
    }

    private Uni<Void> startCqrsFirstParticipantExecute(Logger log, EventMessage eventMessage, Object data,
            Participant participant, Response[] lastResponse) {
        return Uni.createFrom().item(lastResponse[0])
                .chain(resp -> {
                    Response response = Response.ok(data).build();
                    return startCqrsParticipantCall(log, eventMessage, data, participant, response);
                })
                .onItemOrFailure().transform((response, failure)
                        -> {
                    lastResponse[0] = checkParticipantResponseError(log, participant.getName(),
                            response, failure, eventMessage.getErrors());
                    return lastResponse[0];
                }).replaceWithVoid();
    }

    private Uni<Void> startCqrsNextParticipantExecute(Logger log, EventMessage eventMessage, Object data, String previousParticipantName,
            Participant participant, Response[] lastResponse) {
        return Uni.createFrom().item(lastResponse[0])
                .chain(response -> {
                    if (eventMessage.getErrors().isEmpty() && response != null) {
                        try {
                            return startCqrsParticipantCall(log, eventMessage, data, participant, response);
                        } catch (Exception ex) {
                            log.error(previousParticipantName, ex);
                            eventMessage.getErrors().put(previousParticipantName, ex.getMessage());
                            return Uni.createFrom().item(Response.ok().build());
                        }
                    } else {
                        return Uni.createFrom().item(Response.ok().build());
                    }
                })
                .onItemOrFailure().transform((response, failure)
                        -> {
                    lastResponse[0] = checkParticipantResponseError(log, participant.getName(),
                            response, failure, eventMessage.getErrors());
                    return lastResponse[0];
                }).replaceWithVoid();
    }

    private Uni<Response> startCqrsParticipantCall(Logger log, EventMessage eventMessage, Object data, Participant participant, Response response) {
        eventMessage.setExpire(eventMessage.getExpire().plus(participant.getCqrsTimeout(), ChronoUnit.MILLIS));
        Object details = response.readEntity(data.getClass());
        eventMessage.setDetails(details);
        log.debug(getTruncatedString(jsoner.json(log, eventMessage)));
        return participant.getClient().execute(eventMessage.getUuid(), eventMessage.getExpire(), eventMessage.getOperation(), details);
    }

}
