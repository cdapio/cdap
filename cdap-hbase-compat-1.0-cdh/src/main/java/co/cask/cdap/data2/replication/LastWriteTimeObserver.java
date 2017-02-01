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

package co.cask.cdap.data2.replication;

import co.cask.cdap.replication.ReplicationConstants;
import co.cask.cdap.replication.StatusUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseWALObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase coprocessor that tracks WAL writes for all tables to track replication status.
 * For each region the writeTime of the last WAL entry is written to the REPLICATION_STATE table.
 */
public class LastWriteTimeObserver extends BaseWALObserver {
  private HBase10CDHTableUpdater hBase10CDHTableUpdater = null;
  private static final Logger LOG = LoggerFactory.getLogger(LastWriteTimeObserver.class);

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    LOG.info("LastWriteTimeObserver Start received.");
    String tableName = StatusUtils.getReplicationStateTableName(env.getConfiguration());
    HTableInterface htableInterface = env.getTable(TableName.valueOf(tableName));
    hBase10CDHTableUpdater = new HBase10CDHTableUpdater(ReplicationConstants.ReplicationStatusTool.WRITE_TIME_ROW_TYPE,
                                                        env.getConfiguration(), htableInterface);
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    LOG.info("LastWriteTimeObserver Stop received.");
    hBase10CDHTableUpdater.cancelTimer();
  }

  @Override
  public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx, HRegionInfo info,
                           WALKey logKey, WALEdit logEdit) throws IOException {
    if (logKey.getScopes() == null || logKey.getScopes().size() == 0 || !logKey.getClusterIds().isEmpty()) {
      //if replication scope is not set for this entry, do not update write time.
      // This is to save us from cases where some HBase tables do not have replication enabled.
      // Ideally, you would check scopes against REPLICATION_SCOPE_LOCAL, but cell.getFamily() is expensive.
      // also ignore if this logkey was processed by another cluster,
      // which means the write originated on another cluster
      return;
    }
    LOG.debug("Update LastWriteTimeObserver for Table {}:{} for region={}",
              logKey.getTablename().toString(),
              logKey.getWriteTime(),
              logKey.getEncodedRegionName().toString());
    hBase10CDHTableUpdater.updateTime(new String(logKey.getEncodedRegionName(), "UTF-8"), logKey.getWriteTime());
  }
}

