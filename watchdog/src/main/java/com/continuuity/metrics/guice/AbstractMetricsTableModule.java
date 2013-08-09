/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * Base guice module for binding MetricsTableFactory in different runtime mode.
 */
public abstract class AbstractMetricsTableModule extends PrivateModule {

  @Override
  protected final void configure() {
    bindTableHandle();
    bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);

    expose(MetricsTableFactory.class);
  }

  protected void bindTableHandle() {
    bind(OVCTableHandle.class).annotatedWith(MetricsAnnotation.class).to(OVCTableHandle.class);
  }
}
