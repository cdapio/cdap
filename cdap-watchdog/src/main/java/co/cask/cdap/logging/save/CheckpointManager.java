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

package co.cask.cdap.logging.save;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Manages reading/writing of checkpoint information for a topic and partition.
 */
public final class CheckpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);

  private static final byte [] OFFSET_COLNAME = Bytes.toBytes("nextOffset");
  private static final byte [] MAX_TIME_COLNAME = Bytes.toBytes("maxEventTime");

  private final byte [] rowKeyPrefix;
  private final LogSaverTableUtil tableUtil;
  private final TransactionExecutorFactory transactionExecutorFactory;
  private Map<Integer, Checkpoint> lastCheckpoint;

  public CheckpointManager(final LogSaverTableUtil tableUtil,
                           TransactionExecutorFactory txExecutorFactory, String topic, int prefix) {
    this.rowKeyPrefix = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes(topic));
    this.tableUtil = tableUtil;
    this.transactionExecutorFactory = txExecutorFactory;
    this.lastCheckpoint = new HashMap<>();
  }

  private <T> T execute(TransactionExecutor.Function<Table, T> func) {
    try {
      Table table = tableUtil.getMetaTable();
      if (table instanceof TransactionAware) {
        TransactionExecutor txExecutor = Transactions.createTransactionExecutor(transactionExecutorFactory,
                                                                                (TransactionAware) table);
        return txExecutor.execute(func, table);
      } else {
        throw new RuntimeException(String.format("Table %s is not TransactionAware, " +
                                                   "Exception while trying to cast it to TransactionAware. " +
                                                   "Please check why the table is not TransactionAware", table));
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error accessing %s table", tableUtil.getMetaTableName()), e);
    }
  }

  private void execute(TransactionExecutor.Procedure<Table> procedure) {
    try {
      Table table = tableUtil.getMetaTable();
      if (table instanceof TransactionAware) {
        TransactionExecutor txExecutor = Transactions.createTransactionExecutor(transactionExecutorFactory,
                                                                                (TransactionAware) table);
        txExecutor.execute(procedure, table);
      } else {
        throw new RuntimeException(String.format("Table %s is not TransactionAware, " +
                                                   "Exception while trying to cast it to TransactionAware. " +
                                                   "Please check why the table is not TransactionAware", table));
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error accessing %s table", tableUtil.getMetaTableName()), e);
    }
  }

  public void saveCheckpoint(final Map<Integer, Checkpoint> checkpoints) throws Exception {
    // if the checkpoints have not changed, we skip writing to table and return.
    if (lastCheckpoint.equals(checkpoints)) {
      return;
    }

    execute(new TransactionExecutor.Procedure<Table>() {
      @Override
      public void apply(Table table) throws Exception {
        for (Map.Entry<Integer, Checkpoint> entry : checkpoints.entrySet()) {
          byte[] key = Bytes.add(rowKeyPrefix, Bytes.toBytes(entry.getKey()));
          Checkpoint checkpoint = entry.getValue();
          table.put(key, OFFSET_COLNAME, Bytes.toBytes(checkpoint.getNextOffset()));
          table.put(key, MAX_TIME_COLNAME, Bytes.toBytes(checkpoint.getMaxEventTime()));
        }
        // update last checkpoint
        lastCheckpoint = ImmutableMap.copyOf(checkpoints);
      }
    });
    LOG.trace("Saving checkpoints for partitions {}", checkpoints);
  }

  public Map<Integer, Checkpoint> getCheckpoint(final Set<Integer> partitions) throws Exception {
    return execute(new TransactionExecutor.Function<Table, Map<Integer, Checkpoint>>() {
      @Override
      public Map<Integer, Checkpoint> apply(Table table) throws Exception {
        Map<Integer, Checkpoint> checkpoints = new HashMap<Integer, Checkpoint>();
        for (final int partition : partitions) {
          Row result =
            table.get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)));
          checkpoints.put(partition,
                             new Checkpoint(result.getLong(OFFSET_COLNAME, -1),
                                            result.getLong(MAX_TIME_COLNAME, -1)));
        }
        return checkpoints;
      }
    });
  }

  public Checkpoint getCheckpoint(final int partition) throws Exception {
    Checkpoint checkpoint = execute(new TransactionExecutor.Function<Table, Checkpoint>() {
      @Override
      public Checkpoint apply(Table table) throws Exception {
        Row result = table.get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)));
        return new Checkpoint(result.getLong(OFFSET_COLNAME, -1), result.getLong(MAX_TIME_COLNAME, -1));
      }
    });
    LOG.trace("Read checkpoint {} for partition {}", checkpoint, partition);
    return checkpoint;
  }
}
