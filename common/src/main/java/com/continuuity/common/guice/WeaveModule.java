/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.SecureStoreUpdater;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Guice module for providing bindings for Weave. This module requires accessible bindings to
 * {@link CConfiguration}, {@link YarnConfiguration} and {@link LocationFactory}.
 */
public class WeaveModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(WeaveRunnerService.class).to(ReactorWeaveRunnerService.class).in(Scopes.SINGLETON);
    bind(WeaveRunner.class).to(WeaveRunnerService.class);
  }

  /**
   * Provider method for instantiating {@link YarnWeaveRunnerService}.
   */
  @Singleton
  @Provides
  private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration,
                                                               LocationFactory locationFactory) {
    String zkConnectStr = configuration.get(Constants.Zookeeper.QUORUM) +
                          configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");

    // Copy the yarn config and set the max heap ratio.
    YarnConfiguration yarnConfig = new YarnConfiguration(yarnConfiguration);
    yarnConfig.set(Constants.CFG_WEAVE_RESERVED_MEMORY_MB, configuration.get(Constants.CFG_WEAVE_RESERVED_MEMORY_MB));
    YarnWeaveRunnerService runner = new YarnWeaveRunnerService(yarnConfig,
                                                               zkConnectStr,
                                                               LocationFactories.namespace(locationFactory, "weave"));

    // Set JVM options based on configuration
    runner.setJVMOptions(configuration.get(Constants.AppFabric.PROGRAM_JVM_OPTS));

    return runner;
  }


  /**
   * A {@link WeaveRunnerService} that delegates to {@link YarnWeaveRunnerService} and by default always
   * set the process user name based on CConfiguration when prepare to launch application.
   */
  @Singleton
  private static final class ReactorWeaveRunnerService implements WeaveRunnerService {

    private final YarnWeaveRunnerService delegate;
    private final String yarnUser;

    @Inject
    ReactorWeaveRunnerService(YarnWeaveRunnerService delegate, CConfiguration cConf) {
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
    public WeavePreparer prepare(WeaveRunnable runnable) {
      return delegate.prepare(runnable).setUser(yarnUser);
    }

    @Override
    public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay,
                                                 long delay, TimeUnit unit) {
      return delegate.scheduleSecureStoreUpdate(updater, initialDelay, delay, unit);
    }

    @Override
    public WeavePreparer prepare(WeaveRunnable runnable, ResourceSpecification resourceSpecification) {
      return delegate.prepare(runnable, resourceSpecification).setUser(yarnUser);
    }

    @Override
    public WeavePreparer prepare(WeaveApplication application) {
      return delegate.prepare(application).setUser(yarnUser);
    }

    @Override
    public WeaveController lookup(String applicationName, RunId runId) {
      return delegate.lookup(applicationName, runId);
    }

    @Override
    public Iterable<WeaveController> lookup(String applicationName) {
      return delegate.lookup(applicationName);
    }

    @Override
    public Iterable<LiveInfo> lookupLive() {
      return delegate.lookupLive();
    }
  }
}
