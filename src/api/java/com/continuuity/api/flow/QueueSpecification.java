/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.io.Schema;

import java.net.URI;

/**
 * This interface defines the specification for a Queue.
 * <p>
 *   A queue is defined by it's {@link URI} and the {@link Schema}.
 * </p>
 */
public interface QueueSpecification {
  /**
   * @return URI of the queue.
   */
  public URI getURI();

  /**
   * @return Schema of the queue.
   */
  public Schema getSchema();
}
