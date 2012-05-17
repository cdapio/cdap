/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BufferEntry {
  private static final Logger LOG = LoggerFactory.getLogger(BufferEntry.class);
  private final String sourceName;
  private final Iterable<Record> records;

  public BufferEntry(String sourceName, Iterable<Record> records ) {
    this.sourceName = sourceName;
    this.records = records;
  }

  public String getSourceName() {
    return sourceName;
  }

  Iterable<Record> getRecords() {
    return records;
  }

}
