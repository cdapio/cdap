/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.table.OVCTableHandle;

/**
 *
 */
public final class InMemoryMetricsTableModule extends AbstractMetricsTableModule {

  @Override
  protected void bindTableHandle() {
    bind(OVCTableHandle.class).annotatedWith(MetricsAnnotation.class).toInstance(MemoryOVCTableHandle.getInstance());
  }
}
