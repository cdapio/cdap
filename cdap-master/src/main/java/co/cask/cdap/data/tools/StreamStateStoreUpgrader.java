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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Upgrades stream state store table
 */
public class StreamStateStoreUpgrader extends AbstractQueueUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(StreamStateStoreUpgrader.class);
  private final StreamAdmin streamAdmin;

  @Inject
  public StreamStateStoreUpgrader(LocationFactory locationFactory, NamespacedLocationFactory namespacedLocationFactory,
                                  HBaseTableUtil tableUtil, Configuration conf, StreamAdmin streamAdmin) {
    super(locationFactory, namespacedLocationFactory, tableUtil, conf);
    this.streamAdmin = streamAdmin;
  }

  @Override
  void upgrade() throws Exception {
    super.upgrade();
    streamAdmin.upgrade();
  }

  @Override
  protected TableId getTableId() {
    return StreamUtils.getStateStoreTableId(Constants.DEFAULT_NAMESPACE_ID);
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
    QueueName queueName = QueueName.from(oldRowKey);

    if (queueName.isStream() && queueName.getNumComponents() == 1) {
      LOG.debug("Found old stream state {}. Upgrading.", queueName);
      return Id.Stream.from(Constants.DEFAULT_NAMESPACE, queueName.getFirstComponent());
    } else {
      LOG.warn("Unknown format for queueName: {}. Skipping.", queueName);
    }

    // skip unknown rows or rows that may already have a namespace in the QueueName
    return null;
  }
}
