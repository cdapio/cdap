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
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Upgrades queue.config table
 */
public class QueueConfigUpgrader extends AbstractQueueUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(QueueConfigUpgrader.class);
  private final QueueAdmin queueAdmin;

  @Inject
  public QueueConfigUpgrader(LocationFactory locationFactory, NamespacedLocationFactory namespacedLocationFactory,
                             HBaseTableUtil tableUtil, Configuration conf, QueueAdmin queueAdmin) {
    super(locationFactory, namespacedLocationFactory, tableUtil, conf);
    this.queueAdmin = queueAdmin;
  }

  @Override
  void upgrade() throws Exception {
    super.upgrade();
    queueAdmin.upgrade();
  }

  @Override
  protected TableId getTableId() {
    String dsName = Joiner.on(".").join(Constants.SYSTEM_NAMESPACE, QueueConstants.QUEUE_CONFIG_TABLE_NAME);
    return TableId.from(Constants.DEFAULT_NAMESPACE, dsName);
  }

  @Nullable
  @Override
  protected byte[] processRowKey(byte[] oldRowKey) {
    LOG.debug("Processing queue config for: {}", Bytes.toString(oldRowKey));
    QueueName queueName = fromRowKey(oldRowKey);
    LOG.debug("Processing row key for  queue name: {}", queueName);
    return queueName == null ? null : queueName.toBytes();
  }

  @Nullable
  private QueueName fromRowKey(byte[] oldRowKey) {
    QueueName queueName = QueueName.from(oldRowKey);

    if (queueName.getNumComponents() == 4) {
      LOG.debug("Found old queue config {}. Upgrading.", queueName);
      return QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE,
                                   queueName.getFirstComponent(),
                                   queueName.getSecondComponent(),
                                   queueName.getThirdComponent(),
                                   queueName.getFourthComponent());
    } else if (queueName.getNumComponents() == 5) {
      LOG.debug("Queue config for {} seems to already have been upgraded. Skipping.", queueName);
    } else {
      LOG.warn("Unknown format for queue config {}. Skipping.", queueName);
    }
    // skip unknown rows or rows that may already have a namespace in the QueueName
    return null;
  }
}
