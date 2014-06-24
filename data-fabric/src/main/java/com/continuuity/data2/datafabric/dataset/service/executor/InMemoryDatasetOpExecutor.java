package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * In-memory implementation of {@link DatasetOpExecutor}.
 */
public class InMemoryDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {

  private final RemoteDatasetFramework client;

  @Inject
  public InMemoryDatasetOpExecutor(RemoteDatasetFramework client) {
    this.client = client;
  }

  @Override
  public boolean exists(String instanceName) throws Exception {
    return getAdmin(instanceName).exists();
  }

  @Override
  public DatasetSpecification create(String instanceName, DatasetTypeMeta typeMeta, DatasetProperties props)
    throws Exception {

    DatasetDefinition def = client.getDatasetDefinition(typeMeta, null);

    if (def == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    DatasetSpecification spec = def.configure(instanceName, props);
    DatasetAdmin admin = def.getAdmin(spec, null);
    admin.create();

    return spec;
  }

  @Override
  public void drop(DatasetSpecification spec, DatasetTypeMeta typeMeta) throws Exception {
    DatasetDefinition def = client.getDatasetDefinition(typeMeta, null);

    if (def == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    DatasetAdmin admin = def.getAdmin(spec, null);
    admin.drop();
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
