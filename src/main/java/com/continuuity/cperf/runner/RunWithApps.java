package com.continuuity.cperf.runner;

import com.continuuity.api.Application;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface RunWithApps {
    /**
     * @return a Runner class (must have a constructor that takes a single Class to run)
     */
    Class<? extends Application>[] value();
}