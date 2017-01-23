/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.replication.hbase10cdh550;

import co.cask.cdap.replication.ReplicationConstants;
import co.cask.cdap.replication.StatusUtils;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * HBase Coprocessor that tracks write time of the WAL entries successfully replicated to a Slave Cluster.
 * For each region the writeTime from the last WAL Entry replicated is updated to the REPLICATION_STATE table.
 */
public class LastReplicateTimeObserver extends BaseRegionServerObserver {
  private HBase10CDH550TableUpdater hBase10CDH550TableUpdater = null;
  private static final Logger LOG = LoggerFactory.getLogger(LastReplicateTimeObserver.class);

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    LOG.info("LastReplicateTimeObserver Start received.");
    String tableName = StatusUtils.getReplicationStateTableName(env.getConfiguration());
    HTableInterface htableInterface = env.getTable(TableName.valueOf(tableName));
    hBase10CDH550TableUpdater =
      new HBase10CDH550TableUpdater(ReplicationConstants.ReplicationStatusTool.REPLICATE_TIME_ROW_TYPE,
                                    env.getConfiguration(), htableInterface);
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    LOG.info("LastReplicateTimeObserver Stop received.");
    hBase10CDH550TableUpdater.cancelTimer();
  }

  @Override
  public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                      List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
    for (AdminProtos.WALEntry entry : entries) {
      LOG.debug("Update LastReplicateTimeObserver for Table {}:{} for region {}",
                entry.getKey().getTableName().toStringUtf8(),
                entry.getKey().getWriteTime(),
                entry.getKey().getEncodedRegionName().toStringUtf8());
      hBase10CDH550TableUpdater.updateTime(entry.getKey().getEncodedRegionName().toStringUtf8(),
                                           entry.getKey().getWriteTime());
    }
  }
}

