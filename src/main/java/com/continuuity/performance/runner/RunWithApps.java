package com.continuuity.performance.runner;

import com.continuuity.api.Application;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to assign applications to performance tests.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface RunWithApps {
    /**
     * Applications to be deployed and tested during performance test runs.
     */
    Class<? extends Application>[] value();
}
