package com.continuuity.data2.transaction.runtime;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Binds the {@link com.continuuity.common.metrics.MetricsCollectionService} to a no-op implementation.
 */
// TODO: replace the common MetricsCollectionService interface with a transactions specific one
public class TransactionMetricsModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Singleton.class);
  }
}
