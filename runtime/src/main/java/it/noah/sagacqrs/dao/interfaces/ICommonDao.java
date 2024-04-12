/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.dao.interfaces;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import it.noah.sagacqrs.dao.dto.QueryParameters;
import it.noah.sagacqrs.dao.dto.QueryResponse;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 *
 * @author NATCRI
 */
public interface ICommonDao {

    default Uni<QueryResponse> execute(SqlConnection conn, String query) {
        return doExecute(conn.preparedQuery(query).execute());
    }

    default Uni<QueryResponse> execute(SqlConnection conn, String query, QueryParameters parameters) {
        return doExecute(conn.preparedQuery(query).execute(Tuple.from(parameters.getParams())));
    }

    default Uni<QueryResponse> doExecute(Uni<RowSet<Row>> execution) {
        OffsetDateTime now = OffsetDateTime.now();
        return execution.onItemOrFailure().transform((rowSet, error) -> {
            QueryResponse response = new QueryResponse();
            response.setExecutionTime(Duration.between(now, OffsetDateTime.now()).toMillis() + "ms");
            if (error != null) {
                response.setError(error.getMessage());
            } else {
                response.setRowSet(rowSet);
            }
            return response;
        });
    }

    default Uni<Integer> countAffectedRows(Uni<QueryResponse> queryResponse) {
        return queryResponse.onItem().transform(res -> {
            RowSet<Row> rowSet = res.getRowSet();
            int total = 0;
            do {
                total += rowSet.rowCount();
            } while ((rowSet = rowSet.next()) != null);
            return total;
        });
    }

    default String getListOfStringAsString(List<String> items) {
        String result = "";
        if (items != null && !items.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String item : items) {
                sb.append(",'").append(item).append("'");
            }
            result = sb.toString().substring(1);
        }
        return result;
    }

    default String getTruncatedString(String text) {
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

}
