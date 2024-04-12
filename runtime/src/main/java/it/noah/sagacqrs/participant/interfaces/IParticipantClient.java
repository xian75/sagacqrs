/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.participant.interfaces;

import io.smallrye.mutiny.Uni;
import it.noah.sagacqrs.entity.Transaction;
import jakarta.inject.Named;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import java.time.OffsetDateTime;
import java.util.List;

/**
 *
 * @author NATCRI
 */
public interface IParticipantClient {

    default String getName() {
        try {
            return getClass().getAnnotation(Named.class).value();
        } catch (Exception ex) {
            return getClass().getSimpleName();
        }
    }

    @POST
    @Path("/execute")
    Uni<Response> execute(@QueryParam(value = "uuid") String uuid, @QueryParam(value = "expire") OffsetDateTime expire,
            @QueryParam(value = "operation") String operation, Object data);

    @POST
    @Path("/finalize")
    Uni<Response> finalizeCommitAndRollback(List<Transaction> transactions);

}
