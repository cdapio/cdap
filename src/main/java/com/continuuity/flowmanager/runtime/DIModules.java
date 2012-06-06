package com.continuuity.flowmanager.runtime;

import com.continuuity.flowmanager.flowmanager.internal.InMemoryHSQLStateChangerModule;
import com.continuuity.flowmanager.metrics.service.InMemoryHSQLFlowMonitorModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

/**
 * A collection of depenedency injection modules for components of overlord.
 */
public final class DIModules {

  /**
   * Provides binding for an in memory hsql implementations.
   * @return {@link Module} instance that is combined from multiple modules.
   */
  public static Module getInMemoryHSQLModules() {
    return Modules.combine(
        new InMemoryHSQLStateChangerModule() ,
        new InMemoryHSQLFlowMonitorModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(String.class).annotatedWith(Names.named("Flow Monitor JDBC URL")).toInstance("jdbc:hsqldb:mem:fmdb");
          }
        }
    );
  }

}
