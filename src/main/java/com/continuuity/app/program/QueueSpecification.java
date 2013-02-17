/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.io.Schema;

import java.net.URI;

/**
 * This interface defines the specification for associated with either
 * input or output binds to it's respective {@link URI} and {@link Schema}
 */
public interface QueueSpecification {

  /**
   * @return {@link URI} associated with the queue.
   */
  URI getURI();

  /**
   * @return {@link Schema} associated with the respective {@link URI}
   */
  Schema getSchema();
}
