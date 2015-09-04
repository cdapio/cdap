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
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages reading/writing of checkpoint information for a topic and partition.
 */
public final class CheckpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);

  private static final byte [] OFFSET_COLNAME = Bytes.toBytes("nextOffset");
  private static final byte [] MAX_TIME_COLNAME = Bytes.toBytes("maxEventTime");

  private final Transactional<DatasetContext<Table>, Table> mds;
  private final byte [] rowKeyPrefix;

  public CheckpointManager(final LogSaverTableUtil tableUtil,
                           TransactionExecutorFactory txExecutorFactory, String topic, int prefix) {
    this.rowKeyPrefix = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes(topic));
    this.mds = Transactional.of(txExecutorFactory, new Supplier<DatasetContext<Table>>() {
      @Override
      public DatasetContext<Table> get() {
        try {
          return DatasetContext.of(tableUtil.getMetaTable());
        } catch (Exception e) {
          // there's nothing much we can do here
          throw Throwables.propagate(e);
        }
      }
    });
  }


  public void saveCheckpoint(final int partition, final Checkpoint checkpoint) throws Exception {
    LOG.trace("Saving checkpoint {} for partition {}", checkpoint, partition);
    mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> ctx) throws Exception {
        Table table = ctx.get();
        byte[] key = Bytes.add(rowKeyPrefix, Bytes.toBytes(partition));
        table.put(key, OFFSET_COLNAME, Bytes.toBytes(checkpoint.getNextOffset()));
        table.put(key, MAX_TIME_COLNAME, Bytes.toBytes(checkpoint.getMaxEventTime()));
        return null;
      }
    });
  }

  public Checkpoint getCheckpoint(final int partition) throws Exception {
    Checkpoint checkpoint = mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, Checkpoint>() {
      @Override
      public Checkpoint apply(DatasetContext<Table> ctx) throws Exception {
        Row result =
          ctx.get().get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)));
        return new Checkpoint(result.getLong(OFFSET_COLNAME, -1), result.getLong(MAX_TIME_COLNAME, -1));
      }
    });
    LOG.trace("Read checkpoint {} for partition {}", checkpoint, partition);
    return checkpoint;
  }
}
