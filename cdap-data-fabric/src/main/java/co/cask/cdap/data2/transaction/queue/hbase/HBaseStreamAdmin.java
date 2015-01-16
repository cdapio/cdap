/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.stream.StreamCoordinator;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;

/**
 * admin for streams in hbase.
 */
@Singleton
public class HBaseStreamAdmin extends HBaseQueueAdmin implements StreamAdmin {

  private final StreamCoordinator coordinator;

  @Inject
  public HBaseStreamAdmin(Configuration hConf, CConfiguration cConf, LocationFactory locationFactory,
                          HBaseTableUtil tableUtil, StreamCoordinator coordinator) throws IOException {
    super(hConf, cConf, QueueConstants.QueueType.STREAM, locationFactory, tableUtil);
    this.coordinator = coordinator;
  }

  @Override
  public String getActualTableName(QueueName queueName) {
    if (queueName.isStream()) {
      // <cdap namespace>.system.stream.<stream name>
      return getTableNamePrefix() + "." + queueName.getFirstComponent();
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a stream.");
    }
  }

  @Override
  public boolean doDropTable(QueueName queueName) {
    // separate table for each stream, ok to drop
    return true;
  }

  @Override
  public boolean doTruncateTable(QueueName queueName) {
    // separate table for each stream, ok to truncate
    return true;
  }

  @Override
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    // we don't want eviction CP here, hence overriding
    return ImmutableList.of(tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public StreamConfig getConfig(String streamName) throws IOException {
    return null;
  }

  @Override
  public void updateConfig(StreamConfig config) throws IOException {

  }

  @Override
  public void create(String name) throws IOException {
    super.create(name);
    coordinator.streamCreated(name);
  }
}
