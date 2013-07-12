/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.data.HBaseFilterableOVCTableHandle;
import com.google.inject.Scopes;

/**
 * Guice module for binding Metrics table access in distributed mode. This module is intend for installed by
 * other private module in this package.
 */
final class DistributedMetricsTableModule extends AbstractMetricsTableModule {

  @Override
  protected void bindTableHandle() {
    bind(OVCTableHandle.class).to(HBaseFilterableOVCTableHandle.class).in(Scopes.SINGLETON);
  }
}
