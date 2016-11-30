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
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Upgrades stream state store table.
 *
 * Modifies all row keys from "namespace:foo/stream:bar".getBytes() to "stream:foo.bar".getBytes().
 */
public class StreamStateStoreUpgrader extends AbstractQueueUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(StreamStateStoreUpgrader.class);

  @Inject
  public StreamStateStoreUpgrader(LocationFactory locationFactory, HBaseTableUtil tableUtil,
                                  NamespacedLocationFactory namespacedLocationFactory, Configuration conf,
                                  NamespaceQueryAdmin namespaceQueryAdmin, Impersonator impersonator) {
    super(locationFactory, namespacedLocationFactory, tableUtil, conf, namespaceQueryAdmin, impersonator);
  }

  @Override
  protected Multimap<NamespaceId, TableId> getTableIds() throws Exception {
    Multimap<NamespaceId, TableId> tableIdsMap = HashMultimap.create();
    for (NamespaceMeta namespaceMeta : namespaceQueryAdmin.list()) {
      TableId tableId = StreamUtils.getStateStoreTableId(namespaceMeta.getNamespaceId());
      // need to convert TableId. See javadoc of StreamUtils.getStateStoreTableId
      TableId convertedTableId =
        tableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());
      tableIdsMap.put(namespaceMeta.getNamespaceId(), convertedTableId);
    }
    return tableIdsMap;
  }

  @Nullable
  @Override
  protected byte[] processRowKey(byte[] oldRowKey) {
    LOG.debug("Processing stream state for: {}", Bytes.toString(oldRowKey));
    Id.Stream streamId = fromRowKey(oldRowKey);
    LOG.debug("Upgrading stream state for: {}", streamId);
    return streamId == null ? null : streamId.toBytes();
  }

  @Nullable
  private Id.Stream fromRowKey(byte[] oldRowKey) {
    String oldKey = Bytes.toString(oldRowKey);
    String[] oldParts = oldKey.split("/");
    if (oldParts.length != 2) {
      LOG.warn("Unknown format for row key: {}. Expected namespace:foo/stream:bar.", oldKey);
      return null;
    }

    if (!oldParts[0].startsWith("namespace:") || !oldParts[1].startsWith("stream:")) {
      LOG.warn("Unknown format for row key: {}. Expected namespace:foo/stream:bar.", oldKey);
      return null;
    }

    String namespace = oldParts[0].substring("namespace:".length());
    String stream = oldParts[1].substring("stream:".length());
    return Id.Stream.from(namespace, stream);
  }
}
