package com.continuuity.app.services;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Dummy Twill Runner Service for TwillRunnerService injection in SingleNode
 */
public final class DummyTwillRunnerService implements TwillRunnerService {

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return null;
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return null;
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    return null;
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    return null;
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    return null;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return null;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay, long delay, TimeUnit unit) {
    return null;
  }

  @Override
  public ListenableFuture<State> start() {
    return null;
  }

  @Override
  public State startAndWait() {
    return null;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public State state() {
    return null;
  }

  @Override
  public ListenableFuture<State> stop() {
    return null;
  }

  @Override
  public State stopAndWait() {
    return null;
  }

  @Override
  public void addListener(Listener listener, Executor executor) {

  }
}
