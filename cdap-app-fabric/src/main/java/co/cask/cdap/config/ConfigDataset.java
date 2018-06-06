/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Dataset for configs. This dataset does not wrap its operations in a transaction. It is up to the caller
 * to decide what operations belong in a transaction.
 */
public class ConfigDataset {
  static final DatasetId CONFIG_STORE_DATASET_INSTANCE_ID =
    NamespaceId.SYSTEM.dataset(Constants.ConfigStore.CONFIG_TABLE);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final byte[] PROPERTY_COLUMN = Bytes.toBytes("properties");


  private final Table table;

  private ConfigDataset(Table table) {
    this.table = table;
  }

  public static ConfigDataset get(DatasetContext datasetContext, DatasetFramework dsFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, CONFIG_STORE_DATASET_INSTANCE_ID,
                                                    Table.class.getName(), DatasetProperties.EMPTY);
      return new ConfigDataset(table);
    } catch (IOException | DatasetManagementException e) {
      throw new RuntimeException(e);
    }
  }

  public void create(String namespace, String type, Config config) throws ConfigExistsException {
    boolean success = table.compareAndSwap(rowKey(namespace, type, config.getId()), PROPERTY_COLUMN, null,
                                           Bytes.toBytes(GSON.toJson(config.getProperties())));
    if (!success) {
      throw new ConfigExistsException(namespace, type, config.getId());
    }
  }

  public void createOrUpdate(String namespace, String type, Config config) {
    table.put(rowKey(namespace, type, config.getId()), PROPERTY_COLUMN,
              Bytes.toBytes(GSON.toJson(config.getProperties())));
  }

  public void delete(String namespace, String type, String id) throws ConfigNotFoundException {
    byte[] rowKey = rowKey(namespace, type, id);
    if (table.get(rowKey).isEmpty()) {
      throw new ConfigNotFoundException(namespace, type, id);
    }
    table.delete(rowKey);
  }

  public List<Config> list(String namespace, String type) {
    List<Config> configList = Lists.newArrayList();
    byte[] prefixBytes = rowKeyPrefix(namespace, type);
    try (Scanner rows = table.scan(prefixBytes, Bytes.stopKeyForPrefix(prefixBytes))) {
      Row row;
      while ((row = rows.next()) != null) {
        Map<String, String> properties = GSON.fromJson(Bytes.toString(row.get(PROPERTY_COLUMN)),
                                                       MAP_STRING_STRING_TYPE);
        configList.add(new Config(getPart(row.getRow(), prefixBytes.length), properties));
      }
      return configList;
    }
  }

  public Config get(String namespace, String type, String id) throws ConfigNotFoundException {
    Row row = table.get(rowKey(namespace, type, id));
    if (row.isEmpty()) {
      throw new ConfigNotFoundException(namespace, type, id);
    }
    Map<String, String> propertyMap = GSON.fromJson(Bytes.toString(row.get(PROPERTY_COLUMN)), MAP_STRING_STRING_TYPE);
    return new Config(id, propertyMap);
  }

  public void update(String namespace, String type, Config config) throws ConfigNotFoundException {
    byte[] rowKey = rowKey(namespace, type, config.getId());
    if (table.get(rowKey).isEmpty()) {
      throw new ConfigNotFoundException(namespace, type, config.getId());
    }
    table.put(rowKey, PROPERTY_COLUMN, Bytes.toBytes(GSON.toJson(config.getProperties())));
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
