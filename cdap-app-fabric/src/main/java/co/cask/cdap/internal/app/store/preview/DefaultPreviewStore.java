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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.app.store.preview.PreviewStore;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of the {@link PreviewStore} that stores data in in-memory table
 */
public class DefaultPreviewStore implements PreviewStore {

  private static final DatasetId PREVIEW_TABLE_ID = NamespaceId.SYSTEM.dataset("preview.table");
  private static final byte[] TRACER = Bytes.toBytes("t");
  private static final byte[] PROPERTY = Bytes.toBytes("p");
  private static final byte[] VALUE = Bytes.toBytes("v");

  private final Gson gson = new Gson();
  private final AtomicLong counter = new AtomicLong(0L);

  private final LevelDBTableCore table;
  private final LevelDBTableService service;

  @Inject
  public DefaultPreviewStore(LevelDBTableService service) {
    try {
      this.service = service;
      service.ensureTableExists(PREVIEW_TABLE_ID.getDataset());
      this.table = new LevelDBTableCore(PREVIEW_TABLE_ID.getDataset(), service);
    } catch (IOException e) {
      throw new RuntimeException("Error creating preview table", e);
    }
  }

  @Override
  public void put(final ApplicationId applicationId, final String tracerName, final String propertyName,
                  final Object value) {
    MDSKey mdsKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).add(tracerName).add(counter.getAndIncrement()).build();

    try {
      table.put(mdsKey.getKey(), TRACER, Bytes.toBytes(tracerName), 1L);
      table.put(mdsKey.getKey(), PROPERTY, Bytes.toBytes(propertyName), 1L);
      table.put(mdsKey.getKey(), VALUE, Bytes.toBytes(gson.toJson(value)), 1L);
    } catch (IOException e) {
      throw new RuntimeException("Error while putting in preview store", e);
    }
  }

  @Override
  public Map<String, List<JsonElement>> get(final ApplicationId applicationId, final String tracerName) {
    byte[] startRowKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).add(tracerName).build().getKey();
    byte[] stopRowKey = new MDSKey(Bytes.stopKeyForPrefix(startRowKey)).getKey();

    Map<String, List<JsonElement>> result = new HashMap<>();
    try (Scanner scanner = table.scan(startRowKey, stopRowKey, null, null, null)) {
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
    } catch (IOException e) {
      throw new RuntimeException("Error while reading from preview store ", e);
    }
    return result;
  }

  @Override
  public void remove(final ApplicationId applicationId) {
    byte[] startRowKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).build().getKey();
    byte[] stopRowKey = new MDSKey(Bytes.stopKeyForPrefix(startRowKey)).getKey();
    try {
      table.deleteRange(startRowKey, stopRowKey, null, null);
    } catch (IOException e) {
      throw new RuntimeException("Error while removing the application ", e);
    }
  }

  @VisibleForTesting
  void clear() throws IOException, DatasetManagementException {
    service.dropTable(PREVIEW_TABLE_ID.getDataset());
    service.ensureTableExists(PREVIEW_TABLE_ID.getDataset());
  }
}
