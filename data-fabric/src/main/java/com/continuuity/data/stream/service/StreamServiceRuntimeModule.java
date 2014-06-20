/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.common.runtime.RuntimeModule;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Defines Guice modules in different runtime environments.
 */
public final class StreamServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new StreamServiceModule() {
      @Override
      protected void configure() {
        // For in memory stream, nothing to cleanup
        bind(StreamFileJanitorService.class).to(NoopStreamFileJanitorService.class).in(Scopes.SINGLETON);

        super.configure();
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new StreamServiceModule() {
      @Override
      protected void configure() {
        bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);
        super.configure();
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new StreamServiceModule() {
      @Override
      protected void configure() {
        bind(StreamFileJanitorService.class).to(DistributedStreamFileJanitorService.class).in(Scopes.SINGLETON);
        super.configure();
      }
    };
  }


  /**
   * A {@link StreamFileJanitorService} that does nothing. For in memory stream module only.
   */
  private static final class NoopStreamFileJanitorService extends AbstractService implements StreamFileJanitorService {

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      notifyStopped();
    }
  }
}
