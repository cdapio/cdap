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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Upgrading from CDAP version < 3.3 to CDAP version 3.3.
 * This requires updating the TTL property for DatasetSpecification in the DatasetInstanceMDS table.
 */
public class DatasetSpecificationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSpecificationUpgrader.class);
  private static final Gson GSON = new Gson();
  private static final String TTL_UPDATED = "table.ttl.migrated.to.seconds";

  private final HBaseTableUtil tableUtil;
  private final Configuration conf;

  @Inject
  public DatasetSpecificationUpgrader(HBaseTableUtil tableUtil, Configuration conf) {
    this.tableUtil = tableUtil;
    this.conf = conf;
  }

  /**
   * Updates the TTL in the {@link co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS}
   * table for CDAP versions prior to 3.3.
   * <p>
   * The TTL for {@link DatasetSpecification} was stored in milliseconds.
   * Since the spec (as of CDAP version 3.3) is in seconds, the instance MDS entries must be updated.
   * This is to be called only if the current CDAP version is < 3.3.
   * </p>
   * @throws Exception
   */
  public void upgrade() throws Exception {
    TableId datasetSpecId = tableUtil.createHTableId(NamespaceId.SYSTEM, DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
    try (HBaseAdmin hBaseAdmin = new HBaseAdmin(conf)) {
      if (!tableUtil.tableExists(hBaseAdmin, datasetSpecId)) {
        LOG.error("Dataset instance table does not exist: {}. Should not happen", datasetSpecId);
        return;
      }
    }

    HTable specTable = tableUtil.createHTable(conf, datasetSpecId);

    try {
      ScanBuilder scanBuilder = tableUtil.buildScan();
      scanBuilder.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
      scanBuilder.setMaxVersions();
      try (ResultScanner resultScanner = specTable.getScanner(scanBuilder.build())) {
        Result result;
        while ((result = resultScanner.next()) != null) {
          Put put = new Put(result.getRow());
          for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap :
            result.getMap().entrySet()) {
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnMap : familyMap.getValue().entrySet()) {
              for (Map.Entry<Long, byte[]> columnEntry : columnMap.getValue().entrySet()) {
                Long timeStamp = columnEntry.getKey();
                byte[] colVal = columnEntry.getValue();
                // a deleted dataset can still show up here since BufferingTable doesn't actually delete, but
                // writes a null value. The fact that we need to know that implementation detail here is bad.
                // If we could use Table here instead of HTable, this would be hidden from us.
                if (colVal == null || colVal.length == 0) {
                  continue;
                }
                String specEntry = Bytes.toString(colVal);
                DatasetSpecification specification = GSON.fromJson(specEntry, DatasetSpecification.class);
                DatasetSpecification updatedSpec = updateTTLInSpecification(specification, null);
                colVal = Bytes.toBytes(GSON.toJson(updatedSpec));
                put.add(familyMap.getKey(), columnMap.getKey(), timeStamp, colVal);
              }
            }
          }
          // might not need to put anything if all columns were skipped because they are delete markers.
          if (put.size() > 0) {
            specTable.put(put);
          }
        }
      }
    } finally {
      specTable.flushCommits();
      specTable.close();
    }
  }

  private Map<String, String> updatedProperties(Map<String, String> properties) {
    if (properties.containsKey(Table.PROPERTY_TTL) && !properties.containsKey(TTL_UPDATED)) {
      SortedMap<String, String> updatedProperties = new TreeMap<>(properties);
      long updatedValue = TimeUnit.MILLISECONDS.toSeconds(Long.valueOf(updatedProperties.get(Table.PROPERTY_TTL)));
      updatedProperties.put(Table.PROPERTY_TTL, String.valueOf(updatedValue));
      updatedProperties.put(TTL_UPDATED, "true");
      return updatedProperties;
    }
    return properties;
  }

  @VisibleForTesting
  DatasetSpecification updateTTLInSpecification(DatasetSpecification specification, @Nullable String parentName) {
    Map<String, String> properties = updatedProperties(specification.getProperties());
    List<DatasetSpecification> updatedSpecs = new ArrayList<>();
    for (DatasetSpecification datasetSpecification : specification.getSpecifications().values()) {
      updatedSpecs.add(updateTTLInSpecification(datasetSpecification, specification.getName()));
    }

    String specName = specification.getName();
    if (parentName != null && specification.getName().startsWith(parentName)) {
      specName = specification.getName().substring(parentName.length() + 1);
    }
    return DatasetSpecification.builder(specName,
                                        specification.getType()).properties(properties).datasets(updatedSpecs).build();
  }
}
