/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Default Configuration Store.
 */
public class DefaultConfigStore implements ConfigStore {

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final byte[] PROPERTY_COLUMN = Bytes.toBytes("properties");
  private static final DatasetId CONFIG_STORE_DATASET_INSTANCE_ID =
    NamespaceId.SYSTEM.dataset(Constants.ConfigStore.CONFIG_TABLE);

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public DefaultConfigStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  public static void setupDatasets(DatasetFramework dsFramework) throws DatasetManagementException, IOException {
    dsFramework.addInstance(Table.class.getName(), CONFIG_STORE_DATASET_INSTANCE_ID, DatasetProperties.EMPTY);
  }

  private Table getConfigTable(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework,
                                           CONFIG_STORE_DATASET_INSTANCE_ID,
                                           Table.class.getName(), DatasetProperties.EMPTY);
  }

  @Override
  public void create(final String namespace, final String type, final Config config) throws ConfigExistsException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          boolean success = getConfigTable(context).compareAndSwap(rowKey(namespace, type, config.getId()),
                                                                   PROPERTY_COLUMN, null,
                                                                   Bytes.toBytes(GSON.toJson(config.getProperties())));
          if (!success) {
            throw new ConfigExistsException(namespace, type, config.getId());
          }
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ConfigExistsException.class);
    }
  }

  @Override
  public void createOrUpdate(final String namespace, final String type, final Config config) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          getConfigTable(context).put(rowKey(namespace, type, config.getId()), PROPERTY_COLUMN,
                                      Bytes.toBytes(GSON.toJson(config.getProperties())));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public void delete(final String namespace, final String type, final String id) throws ConfigNotFoundException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          byte[] rowKey = rowKey(namespace, type, id);
          Table configTable = getConfigTable(context);
          if (configTable.get(rowKey).isEmpty()) {
            throw new ConfigNotFoundException(namespace, type, id);
          }
          configTable.delete(rowKey);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ConfigNotFoundException.class);
    }
  }

  @Override
  public List<Config> list(final String namespace, final String type) {
    try {
      return Transactions.execute(transactional, new TxCallable<List<Config>>() {
        @Override
        public List<Config> call(DatasetContext context) throws Exception {
          List<Config> configList = Lists.newArrayList();
          byte[] prefixBytes = rowKeyPrefix(namespace, type);
          try (Scanner rows = getConfigTable(context).scan(prefixBytes, Bytes.stopKeyForPrefix(prefixBytes))) {
            Row row;
            while ((row = rows.next()) != null) {
              Map<String, String> properties = GSON.fromJson(Bytes.toString(row.get(PROPERTY_COLUMN)),
                                                             MAP_STRING_STRING_TYPE);
              configList.add(new Config(getPart(row.getRow(), prefixBytes.length), properties));
            }
            return configList;
          }
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public Config get(final String namespace, final String type, final String id) throws ConfigNotFoundException {
    try {
      return Transactions.execute(transactional, new TxCallable<Config>() {
        @Override
        public Config call(DatasetContext context) throws Exception {
          Row row = getConfigTable(context).get(rowKey(namespace, type, id));
          if (row.isEmpty()) {
            throw new ConfigNotFoundException(namespace, type, id);
          }
          Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(row.get(PROPERTY_COLUMN)),
                                                          MAP_STRING_STRING_TYPE);
          return new Config(id, propertyMap);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ConfigNotFoundException.class);
    }
  }

  @Override
  public void update(final String namespace, final String type, final Config config) throws ConfigNotFoundException {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Table configTable = getConfigTable(context);
          byte[] rowKey = rowKey(namespace, type, config.getId());
          if (configTable.get(rowKey).isEmpty()) {
            throw new ConfigNotFoundException(namespace, type, config.getId());
          }
          configTable.put(rowKey, PROPERTY_COLUMN,
                          Bytes.toBytes(GSON.toJson(config.getProperties())));
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, ConfigNotFoundException.class);
    }
  }

  private byte[] rowKey(String namespace, String type, String id) {
    return getMultipartKey(namespace, type, id);
  }

  private byte[] rowKeyPrefix(String namespace, String type) {
    return getMultipartKey(namespace, type);
  }

  private String getPart(byte[] rowBytes, int offset) {
    int length = Bytes.toInt(rowBytes, offset, Bytes.SIZEOF_INT);
    return Bytes.toString(rowBytes, offset + Bytes.SIZEOF_INT, length);
  }

  private byte[] getMultipartKey(String... parts) {
    int sizeOfParts = 0;
    for (String part : parts) {
      sizeOfParts += part.length();
    }

    byte[] result = new byte[1 + sizeOfParts + (parts.length * Bytes.SIZEOF_INT)];
    Bytes.putByte(result, 0, Constants.ConfigStore.VERSION);

    int offset = 1;
    for (String part : parts) {
      Bytes.putInt(result, offset, part.length());
      offset += Bytes.SIZEOF_INT;
      Bytes.putBytes(result, offset, part.getBytes(), 0, part.length());
      offset += part.length();
    }
    return result;
  }
}
