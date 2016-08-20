/*
 * Copyright © 016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.PreviewId;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store for the preview data.
 */
public class PreviewDataset extends AbstractDataset {
  static final String PREVIEW_TABLE_NAME = "preview.table";
  private static final Gson GSON = new Gson();
  private static final byte[] LOGGER = Bytes.toBytes("l");
  private static final byte[] PROPERTY = Bytes.toBytes("p");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] COUNT_RECORD_TYPE = Bytes.toBytes("c");
  private static final byte[] DATA_RECORD_TYPE = Bytes.toBytes("d");

  private final Table table;

  PreviewDataset(Table table) {
    super(PREVIEW_TABLE_NAME, table);
    this.table = table;
  }

  void put(PreviewId previewId, String loggerName, String propertyName, Object value) {
    MDSKey mdsKey = new MDSKey.Builder().add(previewId.getNamespace())
      .add(previewId.getPreview()).add(loggerName).add(COUNT_RECORD_TYPE).build();
    long recordCount = table.incrementAndGet(mdsKey.getKey(), COUNT_RECORD_TYPE, 1L);

    mdsKey = new MDSKey.Builder().add(previewId.getNamespace())
      .add(previewId.getPreview()).add(loggerName).add(DATA_RECORD_TYPE).add(recordCount).build();

    byte[][] columns = new byte[][] { LOGGER, PROPERTY, VALUE };
    byte[][] values = new byte[][] {
      Bytes.toBytes(loggerName),
      Bytes.toBytes(propertyName),
      Bytes.toBytes(GSON.toJson(value))
    };

    table.put(mdsKey.getKey(), columns, values);
  }

  Map<String, List<String>> get(PreviewId previewId, String loggerName) {
    byte[] startRowKey = new MDSKey.Builder().add(previewId.getNamespace())
      .add(previewId.getPreview()).add(loggerName).add(DATA_RECORD_TYPE).build().getKey();
    byte[] stopRowKey = new MDSKey(Bytes.stopKeyForPrefix(startRowKey)).getKey();

    Map<String, List<String>> result = new HashMap<>();
    try (Scanner scanner = table.scan(startRowKey, stopRowKey)) {
      Row indexRow;
      while ((indexRow = scanner.next()) != null) {
        Map<byte[], byte[]> columns = indexRow.getColumns();
        String propertyName = Bytes.toString(columns.get(PROPERTY));
        String value = Bytes.toString(columns.get(VALUE));
        List<String> values = result.get(propertyName);
        if (values == null) {
          values = new ArrayList<>();
          result.put(propertyName, values);
        }
        values.add(value);
      }
    }
    return result;
  }
}
