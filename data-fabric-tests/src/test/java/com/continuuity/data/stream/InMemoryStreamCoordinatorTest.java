/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.stream.service.StreamServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.junit.BeforeClass;

/**
 *
 */
public class InMemoryStreamCoordinatorTest extends StreamCoordinatorTestBase {

  private static Injector injector;

  @BeforeClass
  public static void init() {
    injector = Guice.createInjector(
      new ConfigModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Scopes.SINGLETON);
        }
      }
    );
  }

  @Override
  protected StreamCoordinator createStreamCoordinator() {
    return injector.getInstance(StreamCoordinator.class);
  }
}
