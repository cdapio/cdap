/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.LocationFactories;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.yarn.YarnTwillRunnerService;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Guice module for providing bindings for Twill. This module requires accessible bindings to
 * {@link CConfiguration}, {@link YarnConfiguration} and {@link LocationFactory}.
 */
public class TwillModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(TwillRunnerService.class).to(ReactorTwillRunnerService.class).in(Scopes.SINGLETON);
    bind(TwillRunner.class).to(TwillRunnerService.class);
  }

  /**
   * Provider method for instantiating {@link YarnTwillRunnerService}.
   */
  @Singleton
  @Provides
  private YarnTwillRunnerService provideYarnTwillRunnerService(CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration,
                                                               LocationFactory locationFactory) {
    String zkConnectStr = configuration.get(Constants.Zookeeper.QUORUM) +
                          configuration.get(Constants.CFG_TWILL_ZK_NAMESPACE, "/weave");

    // Copy the yarn config and set the max heap ratio.
    YarnConfiguration yarnConfig = new YarnConfiguration(yarnConfiguration);
    yarnConfig.set(Constants.CFG_TWILL_RESERVED_MEMORY_MB, configuration.get(Constants.CFG_TWILL_RESERVED_MEMORY_MB));
    YarnTwillRunnerService runner = new YarnTwillRunnerService(yarnConfig,
                                                               zkConnectStr,
                                                               LocationFactories.namespace(locationFactory, "weave"));

    // Set JVM options based on configuration
    runner.setJVMOptions(configuration.get(Constants.AppFabric.PROGRAM_JVM_OPTS));

    return runner;
  }


  /**
   * A {@link TwillRunnerService} that delegates to {@link YarnTwillRunnerService} and by default always
   * set the process user name based on CConfiguration when prepare to launch application.
   */
  @Singleton
  private static final class ReactorTwillRunnerService implements TwillRunnerService {

    private final YarnTwillRunnerService delegate;
    private final String yarnUser;

    @Inject
    ReactorTwillRunnerService(YarnTwillRunnerService delegate, CConfiguration cConf) {
      this.delegate = delegate;
      this.yarnUser = cConf.get(Constants.CFG_YARN_USER, System.getProperty("user.name"));
    }

    @Override
    public ListenableFuture<State> start() {
      return delegate.start();
    }

    @Override
    public State startAndWait() {
      return Futures.getUnchecked(start());
    }

    @Override
    public boolean isRunning() {
      return delegate.isRunning();
    }

    @Override
    public State state() {
      return delegate.state();
    }

    @Override
    public ListenableFuture<State> stop() {
      return delegate.stop();
    }

    @Override
    public State stopAndWait() {
      return Futures.getUnchecked(stop());
    }

    @Override
    public void addListener(Listener listener, Executor executor) {
      delegate.addListener(listener, executor);
    }

    @Override
    public TwillPreparer prepare(TwillRunnable runnable) {
      return delegate.prepare(runnable).setUser(yarnUser);
    }

    @Override
    public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay,
                                                 long delay, TimeUnit unit) {
      return delegate.scheduleSecureStoreUpdate(updater, initialDelay, delay, unit);
    }

    @Override
    public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
      return delegate.prepare(runnable, resourceSpecification).setUser(yarnUser);
    }

    @Override
    public TwillPreparer prepare(TwillApplication application) {
      return delegate.prepare(application).setUser(yarnUser);
    }

    @Override
    public TwillController lookup(String applicationName, RunId runId) {
      return delegate.lookup(applicationName, runId);
    }

    @Override
    public Iterable<TwillController> lookup(String applicationName) {
      return delegate.lookup(applicationName);
    }

    @Override
    public Iterable<LiveInfo> lookupLive() {
      return delegate.lookupLive();
    }
  }
}
