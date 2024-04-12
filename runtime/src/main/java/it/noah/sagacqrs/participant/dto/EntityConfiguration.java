/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.participant.dto;

import io.vertx.mutiny.sqlclient.Row;
import it.noah.sagacqrs.entity.EventAttributes;
import it.noah.sagacqrs.entity.interfaces.IEntity;
import it.noah.sagacqrs.entity.qualifiers.SagaAttribute;
import it.noah.sagacqrs.entity.qualifiers.SagaEntity;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author NATCRI
 */
public class EntityConfiguration {

    private String name;
    private Constructor<IEntity> constructor;
    private String dbTableName;
    private List<String> dbColumns;
    private List<String> dbColumnsExcludedFromInsert;
    private Map<String, Method> dbColumnsAndGetMethods;
    private Map<String, Method> dbColumnsAndSetMethods;
    private Map<String, Method> dbColumnsAndGetRowMethods;

    public EntityConfiguration(Class<IEntity> clazz) throws Throwable {
        name = clazz.getSimpleName();
        constructor = clazz.getDeclaredConstructor();
        SagaEntity sagaEntityAnnotation = clazz.getAnnotation(SagaEntity.class);
        dbTableName = sagaEntityAnnotation.table();
        dbColumns = new ArrayList<>();
        dbColumnsExcludedFromInsert = new ArrayList<>();
        dbColumnsAndGetMethods = new HashMap<>();
        dbColumnsAndSetMethods = new HashMap<>();
        dbColumnsAndGetRowMethods = new HashMap<>();
        Map<String, Method> methods = new HashMap<>();
        for (Method method : clazz.getDeclaredMethods()) {
            methods.put(method.getName(), method);
        }
        Map<String, Method> rowMethods = new HashMap<>();
        for (Method method : Row.class.getDeclaredMethods()) {
            rowMethods.put(method.getName(), method);
        }
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            SagaAttribute sagaAttributeAnnotation = field.getAnnotation(SagaAttribute.class);
            if (sagaAttributeAnnotation != null) {
                dbColumns.add(sagaAttributeAnnotation.name());
                if (!sagaAttributeAnnotation.insertable()) {
                    dbColumnsExcludedFromInsert.add(field.getName());
                }
                String fieldName = field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
                String methodName = "get" + fieldName;
                if (methods.containsKey(methodName)) {
                    dbColumnsAndGetMethods.put(sagaAttributeAnnotation.name(), methods.get(methodName));
                } else {
                    methodName = "is" + fieldName;
                    if (methods.containsKey(methodName)) {
                        dbColumnsAndGetMethods.put(sagaAttributeAnnotation.name(), methods.get(methodName));
                    }
                }
                methodName = "set" + fieldName;
                if (methods.containsKey(methodName)) {
                    dbColumnsAndSetMethods.put(sagaAttributeAnnotation.name(), methods.get(methodName));
                }
                String attributeType = field.getType().getSimpleName();
                String getRowMethodName = "get" + attributeType.substring(0, 1).toUpperCase() + attributeType.substring(1);
                if (rowMethods.containsKey(getRowMethodName)) {
                    dbColumnsAndGetRowMethods.put(sagaAttributeAnnotation.name(), rowMethods.get(getRowMethodName));
                }
            }
        }
        newInstance();
    }

    public String Name() {
        return name;
    }

    public IEntity newInstance() throws Throwable {
        return constructor.newInstance();
    }

    public String dbTable() {
        return dbTableName;
    }

    public List<String> dbColumns() {
        return dbColumns;
    }

    public List<String> dbColumnsExcludedFromInsert() {
        return dbColumnsExcludedFromInsert;
    }

    public Map<String, Object> params(IEntity entity) throws Throwable {
        Map<String, Object> params = new HashMap<>();
        for (String dbColumn : dbColumnsAndGetMethods.keySet()) {
            params.put(dbColumn, ((Method) dbColumnsAndGetMethods.get(dbColumn)).invoke(entity));
        }
        return params;
    }

    public IEntity from(IEntity entity, Row row) throws Throwable {
        for (String dbColumn : dbColumnsAndSetMethods.keySet()) {
            if (dbColumnsAndGetRowMethods.containsKey(dbColumn)) {
                ((Method) dbColumnsAndSetMethods.get(dbColumn)).invoke(entity,
                        ((Method) dbColumnsAndGetRowMethods.get(dbColumn)).invoke(row, dbColumn));
            }
        }
        entity.setOptlock(row.getLong("optlock"));
        entity.setEvent(EventAttributes.initialize(row));
        return entity;
    }
}
