package com.continuuity.runtime;

import com.continuuity.metrics.service.SQLFlowMonitorModule;
import com.continuuity.observer.internal.SQLStateChangerModule;
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
   *
   * @return {@link Module} instance that is combined from multiple modules.
   */
  public static Module getInMemoryHSQLBindings() {
    /**
     * TODO: Figure out the right place. Not sure if this is the right place for this to happen
     */
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    return Modules.combine(
      new SQLStateChangerModule(),
      new SQLFlowMonitorModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(String.class).annotatedWith(Names.named("Flow Monitor JDBC URL")).toInstance("jdbc:hsqldb:mem:fmdb");
        }
      }
    );
  }

  public static Module getFileHSQLBindings() {
    /**
     * TODO: Figure out the right place. Not sure if this is the right place for this to happen
     */
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return Modules.combine(
      new SQLStateChangerModule(),
      new SQLFlowMonitorModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(String.class).annotatedWith(Names.named("Flow Monitor JDBC URL"))
            .toInstance("jdbc:hsqldb:file:/tmp/data/flowmonitordb");
        }
      }
    );
  }

}
