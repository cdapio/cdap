/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.hbase.txprune;


import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;

/**
 * Supplies instances of {@link PruneUpperBoundWriter} implementations.
 */
public class PruneUpperBoundWriterSupplier implements Supplier<PruneUpperBoundWriter> {
  private static final Log LOG = LogFactory.getLog(PruneUpperBoundWriterSupplier.class);

  private static volatile PruneUpperBoundWriter instance;
  private static volatile int refCount = 0;
  private static final Object lock = new Object();

  private final TableName tableName;
  private final DataJanitorState dataJanitorState;
  private final long pruneFlushInterval;

  public PruneUpperBoundWriterSupplier(TableName tableName, DataJanitorState dataJanitorState,
                                       long pruneFlushInterval) {
    this.tableName = tableName;
    this.dataJanitorState = dataJanitorState;
    this.pruneFlushInterval = pruneFlushInterval;
  }

  @Override
  public PruneUpperBoundWriter get() {
    synchronized (lock) {
      if (instance == null) {
        instance = new PruneUpperBoundWriter(tableName, dataJanitorState, pruneFlushInterval);
        instance.startAndWait();
      }
      refCount++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Incrementing Reference Count for PruneUpperBoundWriter " + refCount);
      }
      return instance;
    }
  }

  public void release() {
    synchronized (lock) {
      refCount--;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Decrementing Reference Count for PruneUpperBoundWriter " + refCount);
      }

      if (refCount == 0) {
        try {
          instance.stopAndWait();
        } catch (Exception ex) {
          LOG.warn("Exception while trying to shutdown PruneUpperBoundWriter thread. ", ex);
        } finally {
          // If the thread is still alive (might happen if the thread was blocked on HBase PUT call), interrupt it again
          if (instance.isAlive()) {
            try {
              instance.shutDown();
            } catch (Exception e) {
              LOG.warn("Exception while trying to shutdown PruneUpperBoundWriter thread. ", e);
            }
          }
          instance = null;
        }
      }
    }
  }
}
