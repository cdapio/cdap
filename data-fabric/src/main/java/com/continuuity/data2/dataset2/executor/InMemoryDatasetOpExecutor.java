package com.continuuity.data2.dataset2.executor;

import com.continuuity.data2.datafabric.dataset.DataFabricDatasetManager;
import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * In-memory implementation of {@link DatasetOpExecutor}.
 */
public class InMemoryDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {

  private DataFabricDatasetManager client;

  @Inject
  public InMemoryDatasetOpExecutor(DataFabricDatasetManager client) {
    this.client = client;
  }

  public InMemoryDatasetOpExecutor() {

  }

  public void setClient(DataFabricDatasetManager client) {
    this.client = client;
  }

  @Override
  public boolean exists(String instanceName) throws Exception {
    return getAdmin(instanceName).exists();
  }

  @Override
  public void create(String instanceName) throws Exception {
    getAdmin(instanceName).create();
  }

  @Override
  public void drop(String instanceName) throws Exception {
    getAdmin(instanceName).drop();
  }

  @Override
  public void truncate(String instanceName) throws Exception {
    getAdmin(instanceName).truncate();
  }

  @Override
  public void upgrade(String instanceName) throws Exception {
    getAdmin(instanceName).upgrade();
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }

  private DatasetAdmin getAdmin(String instanceName) throws IOException, DatasetManagementException {
    return client.getAdmin(instanceName, null);
  }
}
