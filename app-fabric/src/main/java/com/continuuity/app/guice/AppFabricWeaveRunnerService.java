/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.concurrent.Executor;

/**
 * A {@link WeaveRunnerService} that delegates to {@link YarnWeaveRunnerService} and by default always
 * set the process user name based on CConfiguration when prepare to launch application.
 */
@Singleton
final class AppFabricWeaveRunnerService implements WeaveRunnerService {

  private final YarnWeaveRunnerService delegate;
  private final String yarnUser;

  @Inject
  AppFabricWeaveRunnerService(YarnWeaveRunnerService delegate, CConfiguration cConf) {
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
