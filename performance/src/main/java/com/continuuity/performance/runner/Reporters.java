package com.continuuity.performance.runner;

import com.continuuity.performance.application.MensaMetricsReporter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for assigning mensa metrics reporters to peformance tests.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface Reporters {
    /**
     * List of reporter classes that send metrics to a mensa server.
     */
    Class<? extends MensaMetricsReporter>[] value();
}
