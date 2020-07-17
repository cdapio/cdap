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
package io.cdap.cdap.internal.app.store.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.data2.dataset2.lib.table.MDSKey;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of the {@link PreviewStore} that stores data in a level db table.
 */
public class DefaultPreviewStore implements PreviewStore {
  private static final DatasetId PREVIEW_TABLE_ID = NamespaceId.SYSTEM.dataset("preview.table");
  private static final byte[] TRACER = Bytes.toBytes("t");
  private static final byte[] PROPERTY = Bytes.toBytes("p");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] RUN = Bytes.toBytes("r");
  private static final byte[] STATUS = Bytes.toBytes("s");

  private final AtomicLong counter = new AtomicLong(0L);

  private final LevelDBTableCore table;
  private final LevelDBTableService service;

  @Inject
  DefaultPreviewStore(LevelDBTableService service) {
    try {
      this.service = service;
      service.ensureTableExists(PREVIEW_TABLE_ID.getDataset());
      this.table = new LevelDBTableCore(PREVIEW_TABLE_ID.getDataset(), service);
    } catch (IOException e) {
      throw new RuntimeException("Error creating preview table", e);
    }
  }

  @Override
  public void put(ApplicationId applicationId, String tracerName, String propertyName, Object value) {
    // PreviewStore is a singleton and we have to create gson for each operation since gson is not thread safe.
    Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(StructuredRecord.class, new PreviewJsonSerializer()).create();
    MDSKey mdsKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).add(tracerName).add(counter.getAndIncrement()).build();

    try {
      table.put(mdsKey.getKey(), TRACER, Bytes.toBytes(tracerName), 1L);
      table.put(mdsKey.getKey(), PROPERTY, Bytes.toBytes(propertyName), 1L);
      table.put(mdsKey.getKey(), VALUE, Bytes.toBytes(gson.toJson(value)), 1L);
    } catch (IOException e) {
      String message = String.format("Error while putting property '%s' for application '%s' and tracer '%s' in" +
                                       " preview table.", propertyName, applicationId, tracerName);
      throw new RuntimeException(message, e);
    }
  }

  @Override
  public Map<String, List<JsonElement>> get(ApplicationId applicationId, String tracerName) {
    // PreviewStore is a singleton and we have to create gson for each operation since gson is not thread safe.
    Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
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
        List<JsonElement> values = result.computeIfAbsent(propertyName, k -> new ArrayList<>());
        values.add(value);
      }
    } catch (IOException e) {
      String message = String.format("Error while reading preview data for application '%s' and tracer '%s'.",
                                     applicationId, tracerName);
      throw new RuntimeException(message, e);
    }
    return result;
  }

  @Override
  public void remove(ApplicationId applicationId) {
    byte[] startRowKey = new MDSKey.Builder().add(applicationId.getNamespace())
      .add(applicationId.getApplication()).build().getKey();
    byte[] stopRowKey = new MDSKey(Bytes.stopKeyForPrefix(startRowKey)).getKey();
    try {
      table.deleteRange(startRowKey, stopRowKey, null, null);
    } catch (IOException e) {
      String message = String.format("Error while removing preview data for application '%s'.", applicationId);
      throw new RuntimeException(message, e);
    }
  }

  @Override
  public void setProgramId(ProgramRunId programRunId) {
    // PreviewStore is a singleton and we have to create gson for each operation since gson is not thread safe.
    Gson gson = new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter()).create();
    MDSKey mdsKey = new MDSKey.Builder().add(programRunId.getNamespace()).add(programRunId.getApplication()).build();
    try {
      table.put(mdsKey.getKey(), RUN, Bytes.toBytes(gson.toJson(programRunId)), 1L);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to put %s into preview store", programRunId), e);
    }
  }

  @Override
  public ProgramRunId getProgramRunId(ApplicationId applicationId) {
    // PreviewStore is a singleton and we have to create gson for each operation since gson is not thread safe.
    Gson gson = new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter()).create();
    MDSKey mdsKey = new MDSKey.Builder().add(applicationId.getNamespace()).add(applicationId.getApplication()).build();

    Map<byte[], byte[]> row = null;
    try {
      row = table.getRow(mdsKey.getKey(), new byte[][]{RUN},
                                             null, null, -1, null);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to get program run id for preview %s", applicationId), e);
    }
    if (!row.isEmpty()) {
      return gson.fromJson(Bytes.toString(row.get(RUN)), ProgramRunId.class);
    }
    return null;
  }

  @Override
  public void setPreviewStatus(ApplicationId applicationId, PreviewStatus previewStatus) {
    // PreviewStore is a singleton and we have to create gson for each operation since gson is not thread safe.
    Gson gson = new GsonBuilder().registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();
    MDSKey mdsKey = new MDSKey.Builder().add(applicationId.getNamespace()).add(applicationId.getApplication()).build();
    try {
      table.put(mdsKey.getKey(), STATUS, Bytes.toBytes(gson.toJson(previewStatus)), 1L);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to put preview status %s for preview %s",
                                               previewStatus, applicationId), e);
    }
  }

  @Override
  public PreviewStatus getPreviewStatus(ApplicationId applicationId) {
    // PreviewStore is a singleton and we have to create gson for each operation since gson is not thread safe.
    Gson gson = new GsonBuilder().registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();
    MDSKey mdsKey = new MDSKey.Builder().add(applicationId.getNamespace()).add(applicationId.getApplication()).build();

    Map<byte[], byte[]> row = null;
    try {
      row = table.getRow(mdsKey.getKey(), new byte[][]{STATUS},
                                             null, null, -1, null);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to get the preview status for preview %s", applicationId), e);
    }
    if (!row.isEmpty()) {
      return gson.fromJson(Bytes.toString(row.get(STATUS)), PreviewStatus.class);
    }
    return null;
  }

  @VisibleForTesting
  void clear() throws IOException {
    service.dropTable(PREVIEW_TABLE_ID.getDataset());
    service.ensureTableExists(PREVIEW_TABLE_ID.getDataset());
  }
}
