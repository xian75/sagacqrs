/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Enum.java to edit this template
 */
package it.noah.sagacqrs.enums;

/**
 *
 * @author NATCRI
 */
public enum TransactionOutcome {

    PENDING("WIP"),
    COMMIT("OK"),
    ROLLBACK("ERR"),
    COMMIT_PENDING(""),
    ROLLBACK_PENDING("");

    private final String outcomeResponse;

    private TransactionOutcome(String outcomeResponse) {
        this.outcomeResponse = outcomeResponse;
    }

    public String getOutcomeResponse() {
        return outcomeResponse;
    }
}
