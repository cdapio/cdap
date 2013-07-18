/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.data.HBaseFilterableOVCTableHandle;
import com.google.inject.Scopes;

/**
 * Guice module for binding Metrics table access in distributed mode. This module is intend for installed by
 * other private module in this package.
 */
public final class DistributedMetricsTableModule extends AbstractMetricsTableModule {

  @Override
  protected void bindTableHandle() {
    // In distributed mode, this module won't be created with the data-fabric one, hence needs to provide a
    // TransactionOracle.
    bind(TransactionOracle.class).to(NoopTransactionOracle.class).in(Scopes.SINGLETON);
    bind(OVCTableHandle.class).annotatedWith(MetricsAnnotation.class).
      to(HBaseFilterableOVCTableHandle.class).in(Scopes.SINGLETON);
  }
}
