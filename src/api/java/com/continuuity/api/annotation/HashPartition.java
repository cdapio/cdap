package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  Used to define the strategy to read data from {@link com.continuuity.api.flow.flowlet.Flowlet}'s input.
 *  The input is partitioned among the running {@link com.continuuity.api.flow.flowlet.Flowlet}s using the
 *  partition key.
 *  If the partition key is not emitted then the input is treated as part of partition zero.
 *  @see RoundRobin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HashPartition {
  /**
   * Declare the name of the partition key for data partitioning to the process methods
   * across multiple instances of {@link com.continuuity.api.flow.flowlet.Flowlet}.
   *
   * @return Name of the partition key.
   */
  String value();

}
