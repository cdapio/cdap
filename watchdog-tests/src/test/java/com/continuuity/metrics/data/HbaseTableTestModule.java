package com.continuuity.metrics.data;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
*
*/
final class HbaseTableTestModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
  }
}
