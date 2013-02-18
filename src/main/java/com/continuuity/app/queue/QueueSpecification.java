/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.queue;

import com.continuuity.api.io.Schema;

import java.net.URI;
import java.util.Set;

/**
 * This interface defines the specification for associated with either
 * input or output binds to it's respective {@link URI} and {@link Schema}
 */
public interface QueueSpecification {

  /**
   * @return {@link URI} associated with the queue.
   */
  QueueName getQueueName();

  /**
   * @return {@link Schema} associated with the respective {@link URI}
   */
  Set<Schema> getSchemas();
}
