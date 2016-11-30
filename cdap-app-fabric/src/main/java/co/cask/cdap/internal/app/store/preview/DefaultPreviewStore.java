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
package co.cask.cdap.internal.app.store.preview;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.store.preview.PreviewStore;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link PreviewStore} that stores data in in-memory table
 */
public class DefaultPreviewStore implements PreviewStore {

  private static final DatasetId PREVIEW_TABLE_ID = NamespaceId.SYSTEM.dataset("preview.table");
  private static final byte[] TRACER = Bytes.toBytes("t");
  private static final byte[] PROPERTY = Bytes.toBytes("p");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[][] COLUMNS = new byte[][] {TRACER, PROPERTY, VALUE };

  private final DatasetFramework dsFramework;
  private final Transactional transactional;
  private final Gson gson = new Gson();
  private final AtomicLong counter = new AtomicLong(0L);

  @Inject
  public DefaultPreviewStore(DatasetFramework framework, TransactionSystemClient txClient) {
    this.dsFramework = framework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(framework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  public void put(final ApplicationId applicationId, final String tracerName, final String propertyName,
                  final Object value) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          put(getPreviewTable(context), applicationId, tracerName, propertyName, value);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public Map<String, List<JsonElement>> get(final ApplicationId applicationId, final String tracerName) {
    try {
      return Transactions.execute(transactional, new TxCallable<Map<String, List<JsonElement>>>() {
        @Override
        public Map<String, List<JsonElement>> call(DatasetContext context) throws Exception {
          return get(getPreviewTable(context), applicationId, tracerName);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public void remove(final ApplicationId applicationId) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          remove(getPreviewTable(context), applicationId);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @VisibleForTesting
  void clear() throws IOException, DatasetManagementException {
    truncate(dsFramework.getAdmin(PREVIEW_TABLE_ID, null));
  }

  private Table getPreviewTable(DatasetContext datasetContext)
    throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, PREVIEW_TABLE_ID, Table.class.getName(),
                                           DatasetProperties.EMPTY);
  }

  private void truncate(@Nullable DatasetAdmin admin) throws IOException {
    if (admin != null) {
      admin.truncate();
    }
  }

  /**
   * Put data into the table based on the application id and tracerName. The rowKey is formed by having the namespace
   * id, application id and tracer name as prefix, following by the count of put operations.
   *
   * @param applicationId application id of the preview run.
   * @param tracerName the name of the {@link co.cask.cdap.api.preview.DataTracer}.
   * @param propertyName the property of the data.
   * @param value the value of the data.
   */
  private void put(Table table, ApplicationId applicationId, String tracerName,
                   String propertyName, Object value) {
    MDSKey mdsKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).add(tracerName).add(counter.getAndIncrement()).build();

    byte[][] values = new byte[][] {
      Bytes.toBytes(tracerName),
      Bytes.toBytes(propertyName),
      Bytes.toBytes(gson.toJson(value))
    };
    table.put(mdsKey.getKey(), COLUMNS, values);
  }

  private Map<String, List<JsonElement>> get(Table table, ApplicationId applicationId, String tracerName) {
    byte[] startRowKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).add(tracerName).build().getKey();
    byte[] stopRowKey = new MDSKey(Bytes.stopKeyForPrefix(startRowKey)).getKey();

    Map<String, List<JsonElement>> result = new HashMap<>();
    try (Scanner scanner = table.scan(startRowKey, stopRowKey)) {
      Row indexRow;
      while ((indexRow = scanner.next()) != null) {
        Map<byte[], byte[]> columns = indexRow.getColumns();
        String propertyName = Bytes.toString(columns.get(PROPERTY));
        JsonElement value = gson.fromJson(Bytes.toString(columns.get(VALUE)), JsonElement.class);
        List<JsonElement> values = result.get(propertyName);
        if (values == null) {
          values = new ArrayList<>();
          result.put(propertyName, values);
        }
        values.add(value);
      }
    }
    return result;
  }

  private void remove(Table table, ApplicationId applicationId) {
    byte[] startRowKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).build().getKey();
    byte[] stopRowKey = new MDSKey(Bytes.stopKeyForPrefix(startRowKey)).getKey();
    try (Scanner scanner = table.scan(startRowKey, stopRowKey)) {
      Row row;
      while ((row = scanner.next()) != null) {
        table.delete(row.getRow());
      }
    }
  }
}
