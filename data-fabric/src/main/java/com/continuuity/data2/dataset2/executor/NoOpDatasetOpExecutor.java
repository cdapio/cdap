package com.continuuity.data2.dataset2.executor;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * NO-OP implementation.
 */
public class NoOpDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }

  @Override
  public boolean exists(String instanceName) throws Exception {
    return false;
  }

  @Override
  public void create(String instanceName) throws Exception {

  }

  @Override
  public void drop(String instanceName) throws Exception {

  }

  @Override
  public void truncate(String instanceName) throws Exception {

  }

  @Override
  public void upgrade(String instanceName) throws Exception {

  }
}
