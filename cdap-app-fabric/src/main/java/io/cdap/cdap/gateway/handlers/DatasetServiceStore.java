/*
 * Copyright © 2014-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Preconditions;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ranges;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.kv.NoTxKeyValueTable;
import io.cdap.cdap.proto.RestartServiceInstancesStatus;
import io.cdap.cdap.proto.RestartServiceInstancesStatus.RestartStatus;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DatasetService Store implements ServiceStore using Datasets without Transaction.
 */
public final class DatasetServiceStore extends AbstractIdleService implements ServiceStore {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetServiceStore.class);
  private static final Gson GSON = new Gson();

  private final DatasetFramework dsFramework;
  private NoTxKeyValueTable table;

  @Inject
  public DatasetServiceStore(@Named("local.ds.framework") DatasetFramework dsFramework) {
    this.dsFramework = dsFramework;
  }

  @Override
  public synchronized Integer getServiceInstance(final String serviceName) {
    String count = Bytes.toString(table.get(Bytes.toBytes(serviceName)));
    return (count != null) ? Integer.valueOf(count) : null;
  }

  @Override
  public synchronized void setServiceInstance(final String serviceName, final int instances) {
    table.put(Bytes.toBytes(serviceName), Bytes.toBytes(String.valueOf(instances)));
  }

  @Override
  protected void startUp() throws Exception {
    final DatasetId serviceStoreDatasetInstanceId =
        NamespaceId.SYSTEM.dataset(Constants.Service.SERVICE_INSTANCE_TABLE_NAME);
    table = Retries.supplyWithRetries(() -> {
      try {
        return DatasetsUtil.getOrCreateDataset(dsFramework, serviceStoreDatasetInstanceId,
            NoTxKeyValueTable.class.getName(),
            DatasetProperties.EMPTY, null);
      } catch (Exception e) {
        // Throwing RetryableException here is just to make it retry getting the dataset
        // an exception here usually means there is an hbase problem
        LOG.warn("Error getting service store dataset {}. Will retry after some time: {}",
            serviceStoreDatasetInstanceId, e.getMessage());
        throw new RetryableException(e);
      }
    }, RetryStrategies.exponentialDelay(1, 30, TimeUnit.SECONDS));
  }

  @Override
  protected void shutDown() throws Exception {
    table.close();
  }

  @Override
  public synchronized void setRestartInstanceRequest(String serviceName, long startTimeMs,
      long endTimeMs,
      boolean isSuccess, int instanceId) {
    Preconditions.checkNotNull(serviceName, "Service name should not be null.");
    Preconditions.checkArgument(instanceId >= 0,
        "Instance id has to be greater than or equal to zero.");

    RestartStatus status = isSuccess ? RestartStatus.SUCCESS : RestartStatus.FAILURE;
    RestartServiceInstancesStatus restartStatus =
        new RestartServiceInstancesStatus(serviceName, startTimeMs, endTimeMs, status,
            ImmutableSet.of(instanceId));
    String toJson = GSON.toJson(restartStatus, RestartServiceInstancesStatus.class);

    table.put(Bytes.toBytes(serviceName + "-restart"), Bytes.toBytes(toJson));
  }

  @Override
  public synchronized void setRestartAllInstancesRequest(String serviceName, long startTimeMs,
      long endTimeMs,
      boolean isSuccess) {
    Preconditions.checkNotNull(serviceName, "Service name should not be null.");

    RestartStatus status = isSuccess ? RestartStatus.SUCCESS : RestartStatus.FAILURE;
    Integer serviceInstance = getServiceInstance(serviceName);
    int instanceCount = (serviceInstance == null) ? 0 : serviceInstance;
    Set<Integer> instancesToRestart = Ranges.closedOpen(0, instanceCount)
        .asSet(DiscreteDomains.integers());

    RestartServiceInstancesStatus restartStatus =
        new RestartServiceInstancesStatus(serviceName, startTimeMs, endTimeMs, status,
            instancesToRestart);
    String toJson = GSON.toJson(restartStatus, RestartServiceInstancesStatus.class);

    table.put(Bytes.toBytes(serviceName + "-restart"), Bytes.toBytes(toJson));
  }

  @Override
  public synchronized RestartServiceInstancesStatus getLatestRestartInstancesRequest(
      String serviceName)
      throws IllegalStateException {
    String jsonString = Bytes.toString(table.get(Bytes.toBytes(serviceName + "-restart")));
    if (jsonString == null) {
      throw new IllegalStateException("Unable to find latest restart request for " + serviceName);
    }
    return GSON.fromJson(jsonString, RestartServiceInstancesStatus.class);
  }
}
