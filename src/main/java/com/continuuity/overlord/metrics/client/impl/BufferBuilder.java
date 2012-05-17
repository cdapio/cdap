/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *
 */
public class BufferBuilder extends ArrayList<BufferEntry> {
  private static final Logger LOG = LoggerFactory.getLogger(BufferBuilder.class);
  private static final long serialVersionUID = 2321313131L;

  public boolean add(String sourceName, Iterable<Record> records) {
    return add(new BufferEntry(sourceName, records));
  }

  public Buffer getBuffer() {
    return new Buffer(this);
  }
}
