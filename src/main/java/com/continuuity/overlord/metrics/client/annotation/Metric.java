/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.annotation;

import java.lang.annotation.*;

/**
 * Annontation for specifying Metric. Currently can be applied
 * to only FIELD.
 */
@Documented
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Metric {

    public enum Type {
      COUNTER,
      CUMULATIVE
    }

    Type type() default Type.COUNTER;

    /** Provides description of the Counter */
    String description() default "";

    /** Flowlet name as provided in annotation */
    String name();
}
