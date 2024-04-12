package it.noah.sagacqrs.participant.interfaces;

import io.smallrye.mutiny.Uni;
import it.noah.sagacqrs.entity.Transaction;
import it.noah.sagacqrs.participant.ParticipantConfigurator;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.OffsetDateTime;
import java.util.List;

/**
 *
 * @author NATCRI
 */
public interface IParticipantServer {

    public Uni<Object> execute(@QueryParam(value = "uuid") String uuid, @QueryParam(value = "expire") OffsetDateTime expire,
            @QueryParam(value = "operation") String operation, Object data);

    public ParticipantConfigurator getConfigurator();

    @POST
    @Path("/execute")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    default Uni<Response> executeOperation(@QueryParam(value = "uuid") String uuid, @QueryParam(value = "expire") OffsetDateTime expire,
            @QueryParam(value = "operation") String operation, Object data) {
        return execute(uuid, expire, operation, data).onItemOrFailure().transform((result, failure) -> {
            if (failure != null) {
                return Response.accepted(failure.getMessage()).build();
            } else {
                return Response.ok(result).build();
            }
        });
    }

    default Uni<Object> throwNoOperationFound(String operation) {
        return Uni.createFrom().voidItem().onItem().transform(t -> {
            throw new RuntimeException(operation + " operation not found");
        });
    }

    @POST
    @Path("/finalize")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    default Uni<Response> finalizeOperations(List<Transaction> transactions) {
        return getConfigurator().finalizeOperations(transactions);
    }

}
