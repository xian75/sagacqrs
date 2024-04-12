/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.dao.dto;

import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import java.io.Serializable;

/**
 *
 * @author NATCRI
 */
public class QueryResponse implements Serializable {

    private static final long serialVersionUID = -3767928074251614265L;

    private RowSet<Row> rowSet;
    private String executionTime;
    private String error;

    public RowSet<Row> getRowSet() {
        return rowSet;
    }

    public void setRowSet(RowSet<Row> rowSet) {
        this.rowSet = rowSet;
    }

    public String getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(String executionTime) {
        this.executionTime = executionTime;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

}
