package io.cdap.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate the connector type of a plugin.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Connector {
  /**
   * Default connector type name.
   */
  String DEFAULT_TYPE = "connector";

  /**
   * Returns the name of the connector type. Default is 'connector'.
   */
  String type() default DEFAULT_TYPE;
}
