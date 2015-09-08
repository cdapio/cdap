/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * Implementation of Business Metadata on top of {@link IndexedTable}.
 */
public class BusinessMetadataDataset extends AbstractDataset {
  // column keys
  public static final String KEYVALUE_COLUMN = "kv";
  public static final String VALUE_COLUMN = "v";

  private final IndexedTable indexedTable;

  public BusinessMetadataDataset(IndexedTable indexedTable) {
    super("ignore", indexedTable);
    this.indexedTable = indexedTable;
  }

  /**
   * Add new business metadata.
   *
   * @param metadataRecord The value of the metadata to be saved.
   */
  public void createBusinessMetadata(BusinessMetadataRecord metadataRecord) {
    String targetType = metadataRecord.getTargetType();
    Id.NamespacedId targetId = metadataRecord.getTargetId();
    String key = metadataRecord.getKey();
    MDSKey mdsKey = getInstanceKey(targetType, targetId, key);

    // Put to the default column.
    write(mdsKey, metadataRecord);
  }

  /**
   * Add new business metadata.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream).
   * @param key The metadata key to be added.
   * @param value The metadata value to be added.
   */
  public void createBusinessMetadata(Id.NamespacedId targetId, String key, String value) {
    createBusinessMetadata(new BusinessMetadataRecord(getTargetType(targetId), targetId, key, value));
  }

  /**
   * Return business metadata based on type, target id, and key.
   *
   * @param targetId The id of the target.
   * @param key The metadata key to get.
   * @return instance of {@link BusinessMetadataRecord} for the target type, id, and key.
   */
  public BusinessMetadataRecord getBusinessMetadata(Id.NamespacedId targetId, String key) {
    String targetType = getTargetType(targetId);
    MDSKey mdsKey = getInstanceKey(targetType, targetId, key);
    Row row = indexedTable.get(mdsKey.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] keyvalue = row.get(KEYVALUE_COLUMN);
    byte[] value = row.get(VALUE_COLUMN);

    return new BusinessMetadataRecord(targetType, targetId, key, Bytes.toString(value));
  }

  private static Id.NamespacedId fromString(String type, String id) {
    if (type.equals(Id.Program.class.getSimpleName())) {
      return Id.Program.fromStrings(id.split("/"));
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      return Id.Application.fromStrings(id.split("/"));
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      // TODO Add code
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      return Id.Stream.fromId(id);
    }
    throw new IllegalArgumentException("Illegal Type of metadata source.");
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} based on key.
   *
   * @param key The metadata value to be found.
   * @return Collection of {@link BusinessMetadataRecord} fits the key.
   */
  public Collection<BusinessMetadataRecord> findBusinessMetadataOnKey(String key) {

    // TODO ADD CODE

    return Lists.newArrayList();
  }

  private void write(MDSKey id, BusinessMetadataRecord record) {
    try {
      Put put = new Put(id.getKey());

      // Now add the index columns.
      put.add(Bytes.toBytes(KEYVALUE_COLUMN), Bytes.toBytes(record.getKey() + ":" + record.getValue()));
      put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(record.getValue()));

      indexedTable.put(put);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Helper method to generate key.
  private MDSKey getInstanceKey(String targetType, Id.NamespacedId targetId, String key) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(targetType);
    builder.add(targetId.toString());
    builder.add(key);

    return builder.build();
  }

  private String getTargetType(Id.NamespacedId namespacedId) {
    if (namespacedId instanceof Id.Program) {
      return Id.Program.class.getSimpleName();
    }
    return namespacedId.getClass().getSimpleName();
  }
}
