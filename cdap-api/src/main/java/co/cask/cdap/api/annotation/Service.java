package co.cask.cdap.api.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotates http handler with the service in which they are exposed.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Service {
  /**
   * Returns the name of the service
   */
  String value();

  int position() default -1;
}
