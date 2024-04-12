/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.dao.dto;

/**
 *
 * @author NATCRI
 */
public class QueryParameters {

    private static final long serialVersionUID = -8838885270780416703L;

    private final Object[] params;

    public QueryParameters(Object[] params) {
        this.params = params;
    }

    public Object[] getParams() {
        return params;
    }

}
