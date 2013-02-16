/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

import com.continuuity.api.flow.QueueSpecification;
import com.continuuity.api.io.Schema;

import java.net.URI;

/**
 *
 */
public class SimpleQueueSpecification implements QueueSpecification {
  private final URI uri;
  private final Schema schema;

  public SimpleQueueSpecification(URI uri, Schema schema) {
    this.uri = uri;
    this.schema = schema;
  }

  @Override
  public URI getURI() {
    return uri;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
