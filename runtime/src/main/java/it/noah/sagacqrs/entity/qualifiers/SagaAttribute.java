package it.noah.sagacqrs.entity.qualifiers;

import jakarta.inject.Qualifier;
import static java.lang.annotation.ElementType.FIELD;
import java.lang.annotation.Retention;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Target;

/**
 *
 * @author NATCRI
 */
@Qualifier
@Retention(RUNTIME)
@Target({FIELD})
public @interface SagaAttribute {

    String name();

    /**
     * When attribute is unmarked or marked as insertable = true then its value
     * is used during entity persist. Otherwise, when attribute is marked as
     * insertable = false then its value is ignored and its default database
     * column value is used during entity persist.
     *
     * @return
     */
    boolean insertable() default true;
}
