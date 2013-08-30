/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.data.LevelDBFilterableOVCTableHandle;

/**
 * Guice module to provide bindings for metrics table access in local mode. It's intend to be
 * installed by other private module in this package.
 */
public final class LocalMetricsTableModule extends AbstractMetricsTableModule {

  protected void bindTableHandle() {
    bind(OVCTableHandle.class).annotatedWith(MetricsAnnotation.class)
      .toInstance(LevelDBFilterableOVCTableHandle.getInstance());
  }
}
