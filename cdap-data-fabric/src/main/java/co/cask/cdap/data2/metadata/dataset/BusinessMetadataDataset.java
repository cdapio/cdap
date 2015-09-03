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
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.BusinessMetadataRecord;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * Implementation of Business Metadata on top of {@link IndexedTable}.
 */
public class BusinessMetadataDataset extends MetadataStoreDataset {

  /**
   * All rows we store use single column of this name. This is same as the one in MetadataStoreDataset.
   */
  private static final byte[] COLUMN = Bytes.toBytes("c");

  // column keys
  public static final String KEYVALUE_COLUMN = "kv";
  public static final String VALUE_COLUMN = "v";

  private final IndexedTable indexedTable;

  public BusinessMetadataDataset(IndexedTable indexedTable) {
    super(indexedTable);
    this.indexedTable = indexedTable;
  }

  /**
   * Add new business metadata.
   *
   * @param metadataRecord The value of the metadata to be saved.
   */
  public void createBusinessMetadata(BusinessMetadataRecord metadataRecord) {
    String targetType = metadataRecord.getTargetType();
    String targetId = metadataRecord.getTargetId();
    String key = metadataRecord.getKey();
    MDSKey mdsKey = getInstanceKey(targetType, targetId, key);

    // Put to the default column.
    write(mdsKey, metadataRecord);
  }

  /**
   * Add new business metadata.
   *
   * @param targetType The target type of the metadata. Could be Application, Program, Dataset, or Stream.
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream).
   * @param key The metadata key to be added.
   * @param value The metadata value to be added.
   */
  public void createBusinessMetadata(String targetType, String targetId, String key, String value) {
    createBusinessMetadata(new BusinessMetadataRecord(targetType, targetId, key, value));
  }

  /**
   * Return business metadata based on type, target id, and key.
   *
   * @param targetType The type of the target: App | Program | Dataset | Stream.
   * @param targetId The id of the target.
   * @param key The metadata key to get.
   * @return instance of {@link BusinessMetadataRecord} for the target type, id, and key.
   */
  public BusinessMetadataRecord getBusinessMetadata(String targetType, String targetId, String key) {
    MDSKey mdsKey = getInstanceKey(targetType, targetId, key);
    return get(mdsKey, BusinessMetadataRecord.class);
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} based on key.
   *
   * @param key The metadata key to be found.
   * @return Collection of {@link BusinessMetadataRecord} fits the key.
   */
  public Collection<BusinessMetadataRecord> findBusineesMetadataOnKey(String key) {

    // TODO ADD CODE

    return Lists.newArrayList();
  }

  @Override
  public <T> void write(MDSKey id, T value) {
    try {
      Put put = new Put(id.getKey());

      BusinessMetadataRecord record = (BusinessMetadataRecord) value;

      // Now add the index columns.
      put.add(Bytes.toBytes(KEYVALUE_COLUMN), Bytes.toBytes(record.getKey() + ":" + record.getValue()));
      put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(record.getKey()));

      // Add to main "c" column for MetadataStoreDataset.
      put.add(COLUMN, serialize(value));

      indexedTable.put(put);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Helper method to generate key.
  private MDSKey getInstanceKey(String targetType, String targetId, String key) {
    // TODO Add code

    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(targetType);
    builder.add(targetId);
    builder.add(key);

    return builder.build();
  }
}
