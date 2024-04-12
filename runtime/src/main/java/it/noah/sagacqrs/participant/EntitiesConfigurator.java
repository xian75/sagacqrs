/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package it.noah.sagacqrs.participant;

import it.noah.sagacqrs.entity.interfaces.IEntity;
import it.noah.sagacqrs.participant.dto.EntityConfiguration;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author NATCRI
 */
@ApplicationScoped
public class EntitiesConfigurator {

    private Map<Class<IEntity>, EntityConfiguration> entities;

    @PostConstruct
    void init() {
        entities = new HashMap<>();
    }

    public void add(Class<IEntity>[] classes) throws Throwable {
        for (Class<IEntity> clazz : classes) {
            if (!entities.containsKey(clazz)) {
                entities.put(clazz, new EntityConfiguration(clazz));
            }
        }
    }

    public EntityConfiguration get(Class<IEntity> clazz) throws Throwable {
        if (entities.containsKey(clazz)) {
            return entities.get(clazz);
        } else {
            throw new Exception("Entity not found");
        }
    }
}
