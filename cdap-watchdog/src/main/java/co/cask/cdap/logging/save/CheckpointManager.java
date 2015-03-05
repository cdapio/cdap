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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

/**
 * Manages reading/writing of checkpoint information for a topic and partition.
 */
public final class CheckpointManager {

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(100);
  private static final byte [] OFFSET_COLNAME = Bytes.toBytes("nextOffset");

  private final Transactional<DatasetContext<Table>, Table> mds;
  private final byte [] rowKeyPrefix;

  public CheckpointManager(final LogSaverTableUtil tableUtil,
                           TransactionExecutorFactory txExecutorFactory, String topic) {
    this.rowKeyPrefix = Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(topic));
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


  public void saveCheckpoint(final int partition, final long nextOffset) throws Exception {
    mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, Void>() {
      @Override
      public Void apply(DatasetContext<Table> ctx) throws Exception {
        ctx.get().put(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)), OFFSET_COLNAME, Bytes.toBytes(nextOffset));
        return null;
      }
    });
  }

  public long getCheckpoint(final int partition) throws Exception {
    return mds.execute(new TransactionExecutor.Function<DatasetContext<Table>, Long>() {
      @Override
      public Long apply(DatasetContext<Table> ctx) throws Exception {
        byte [] result =
          ctx.get().get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)), OFFSET_COLNAME);
        if (result == null) {
          return -1L;
        }

        return Bytes.toLong(result);
      }
    });
  }
}
