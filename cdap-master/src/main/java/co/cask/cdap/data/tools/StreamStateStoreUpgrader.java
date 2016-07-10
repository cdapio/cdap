/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
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
                                  NamespaceQueryAdmin namespaceQueryAdmin) {
    super(locationFactory, namespacedLocationFactory, tableUtil, conf, namespaceQueryAdmin);
  }

  @Override
  protected Iterable<TableId> getTableIds() throws Exception {
    return Lists.transform(namespaceQueryAdmin.list(), new Function<NamespaceMeta, TableId>() {
      @Override
      public TableId apply(NamespaceMeta input) {
        return StreamUtils.getStateStoreTableId(Id.Namespace.from(input.getName()));
      }
    });
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
