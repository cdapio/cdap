package com.continuuity.explore.service;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.Executor;

/**
 *
 */
public class MockExploreService implements ExploreService {

  @Override
  public Handle execute(String statement) throws ExploreException {
    return null;
  }

  @Override
  public Status getStatus(Handle handle) throws ExploreException {
    return null;
  }

  @Override
  public List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException {
    return null;
  }

  @Override
  public List<Row> nextResults(Handle handle, int size) throws ExploreException {
    return null;
  }

  @Override
  public Status cancel(Handle handle) throws ExploreException {
    return null;
  }

  @Override
  public void close(Handle handle) throws ExploreException {

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
