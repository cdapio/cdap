/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.io.Schema;

import java.net.URI;

/**
 *
 */
public interface QueueSpecification {
  public URI getURI();
  public Schema getSchema();
}
