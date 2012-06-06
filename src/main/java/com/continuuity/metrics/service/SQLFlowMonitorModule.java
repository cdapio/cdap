package com.continuuity.metrics.service;

import com.continuuity.common.service.RegisteredService;
import com.google.inject.AbstractModule;

/**
 *
 *
 */
public class SQLFlowMonitorModule extends AbstractModule {
  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {
    bind(FlowMonitorHandler.class).to(SQLFlowMonitorHandler.class);
    bind(RegisteredService.class).to(FlowMonitorRegisteredService.class);
  }
}
