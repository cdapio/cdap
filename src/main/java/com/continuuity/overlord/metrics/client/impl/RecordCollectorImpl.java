/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.Record;
import com.continuuity.overlord.metrics.client.RecordBuilder;
import com.continuuity.overlord.metrics.client.RecordCollector;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 *
 *
 */
public class RecordCollectorImpl implements RecordCollector, Iterable<RecordBuilderImpl> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordCollectorImpl.class);
  private final List<RecordBuilderImpl> recordBuilders = Lists.newArrayList();

  @Override
  public RecordBuilderImpl addRecord(String name) {
    return addRecord(new InfoImpl(name, name+" r"));
  }
  @Override
  public RecordBuilderImpl addRecord(Info info) {
    RecordBuilderImpl recordBuilder =  new RecordBuilderImpl(info);
    recordBuilders.add(recordBuilder);
    return recordBuilder;
  }

  public List<Record> getRecords() {
    List<Record> recs = Lists.newArrayListWithCapacity(recordBuilders.size());
    for (RecordBuilder rb : recordBuilders) {
      Record mr = rb.getRecord();
      if (mr != null) {
        recs.add(mr);
      }
    }
    return recs;
  }

  @Override
  public Iterator<RecordBuilderImpl> iterator() {
    return recordBuilders.iterator();
  }

  @Override
  public void clear() {
    recordBuilders.clear();
  }
}
