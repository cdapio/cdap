/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 *
 */
public class Buffer implements  Iterable<BufferEntry>{
  private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
  private final Iterable<BufferEntry> entries;

  Buffer(Iterable<BufferEntry> entries) {
    this.entries = entries;
  }

  @Override
  public Iterator<BufferEntry> iterator() {
    return entries.iterator();
  }

}
