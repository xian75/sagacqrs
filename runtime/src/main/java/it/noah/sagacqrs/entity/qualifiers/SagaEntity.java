package it.noah.sagacqrs.entity.qualifiers;

import jakarta.inject.Qualifier;
import static java.lang.annotation.ElementType.TYPE;
import java.lang.annotation.Retention;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Target;

/**
 * Specify that the entity participates in saga transactions
 *
 * @author NATCRI
 */
@Qualifier
@Retention(RUNTIME)
@Target({TYPE})
public @interface SagaEntity {

    /**
     * The required name of the table into the database. If the schema is not
     * public, don't forget to specify its name before the name: schema.table
     *
     * @return
     */
    String table();
}
