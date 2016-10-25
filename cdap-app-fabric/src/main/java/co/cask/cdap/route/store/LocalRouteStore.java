/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.route.store;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

/**
 * RouteStore where the routes are stored in a table for persistence. This is intended for use in SDK mode
 * where performance is not a big issue but we still need persistence of configuration routes across restarts.
 */
public class LocalRouteStore implements RouteStore  {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_INTEGER_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final DatasetId ROUTE_STORE_DATASET_INSTANCE_ID = NamespaceId.SYSTEM.dataset("routestore");

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public LocalRouteStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)), RetryStrategies.retryOnConflict(20, 100));
  }

  private KeyValueTable getRouteTable(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, ROUTE_STORE_DATASET_INSTANCE_ID,
                                           KeyValueTable.class.getName(), DatasetProperties.EMPTY);
  }

  @Override
  public void store(final ProgramId serviceId, final RouteConfig routeConfig) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          getRouteTable(context).write(ServiceDiscoverable.getName(serviceId), GSON.toJson(routeConfig.getRoutes()));
        }
      });
    } catch (TransactionFailureException ex) {
      throw Transactions.propagate(ex);
    }
  }

  @Override
  public void delete(final ProgramId serviceId) throws NotFoundException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          byte[] key = Bytes.toBytes(ServiceDiscoverable.getName(serviceId));
          KeyValueTable kvTable = getRouteTable(context);
          if (kvTable.read(key) == null) {
            throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
          }
          kvTable.delete(key);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, NotFoundException.class);
    }
  }

  @Override
  public RouteConfig fetch(final ProgramId serviceId) {
    try {
      return Transactions.execute(transactional, new TxCallable<RouteConfig>() {
        @Override
        public RouteConfig call(DatasetContext context) throws Exception {
          byte[] value = getRouteTable(context).read(ServiceDiscoverable.getName(serviceId));
          if (value == null) {
            return new RouteConfig(Collections.<String, Integer>emptyMap());
          }

          Map<String, Integer> routeConfigs = GSON.fromJson(Bytes.toString(value), MAP_STRING_INTEGER_TYPE);
          return new RouteConfig(routeConfigs);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public void close() throws Exception {
    // nothing to close
  }
}
