package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  Used to define the strategy to read data from {@link com.continuuity.api.flow.flowlet.Flowlet}'s input.
 *  The input is processed by the {@link com.continuuity.api.flow.flowlet.Flowlet}s in a round-robin manner.
 *  @see HashPartition
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RoundRobin {

}
