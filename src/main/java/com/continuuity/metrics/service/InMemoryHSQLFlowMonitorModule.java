package com.continuuity.metrics.service;

import com.google.inject.AbstractModule;

/**
 *
 *
 */
public class InMemoryHSQLFlowMonitorModule extends AbstractModule {
  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {
    bind(FlowMonitorHandler.class).to(SQLFlowMonitorHandler.class);
  }
}
