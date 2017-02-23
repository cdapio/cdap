/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.txprune;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.util.TxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class executes one run of transaction pruning every time it is invoked.
 * Typically, this class will be scheduled to run periodically.
 */
@SuppressWarnings("WeakerAccess")
public class TransactionPruningRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPruningRunnable.class);

  private final TransactionManager txManager;
  private final Map<String, TransactionPruningPlugin> plugins;
  private final long txMaxLifetimeMillis;
  private final long txPruneBufferMillis;

  public TransactionPruningRunnable(TransactionManager txManager, Map<String, TransactionPruningPlugin> plugins,
                                    long txMaxLifetimeMillis, long txPruneBufferMillis) {
    this.txManager = txManager;
    this.plugins = plugins;
    this.txMaxLifetimeMillis = txMaxLifetimeMillis;
    this.txPruneBufferMillis = txPruneBufferMillis;
  }

  @Override
  public void run() {
    try {
      // TODO: TEPHRA-159 Start a read only transaction here
      Transaction tx = txManager.startShort();
      txManager.abort(tx);

      if (tx.getInvalids().length == 0) {
        LOG.info("Invalid list is empty, not running transaction pruning");
        return;
      }

      long now = getTime();
      long inactiveTransactionBound = TxUtils.getInactiveTxBound(now, txMaxLifetimeMillis + txPruneBufferMillis);
      LOG.info("Starting invalid prune run for time {} and inactive transaction bound {}",
               now, inactiveTransactionBound);

      List<Long> pruneUpperBounds = new ArrayList<>();
      for (Map.Entry<String, TransactionPruningPlugin> entry : plugins.entrySet()) {
        String name = entry.getKey();
        TransactionPruningPlugin plugin = entry.getValue();
        try {
          LOG.debug("Fetching prune upper bound using plugin {}", name);
          long pruneUpperBound = plugin.fetchPruneUpperBound(now, inactiveTransactionBound);
          LOG.debug("Got prune upper bound {} from plugin {}", pruneUpperBound, name);
          pruneUpperBounds.add(pruneUpperBound);
        } catch (Exception e) {
          LOG.error("Aborting invalid prune run for time {} due to exception from plugin {}", now, name, e);
          return;
        }
      }

      long minPruneUpperBound = Collections.min(pruneUpperBounds);
      LOG.info("Got minimum prune upper bound {} across all plugins", minPruneUpperBound);
      if (minPruneUpperBound <= 0) {
        LOG.info("Not pruning invalid list since minimum prune upper bound ({}) is less than 1", minPruneUpperBound);
        return;
      }

      long[] invalids = tx.getInvalids();
      TreeSet<Long> toTruncate = new TreeSet<>();
      LOG.debug("Invalid list: {}", invalids);
      for (long invalid : invalids) {
        if (invalid <= minPruneUpperBound) {
          toTruncate.add(invalid);
        }
      }
      if (toTruncate.isEmpty()) {
        LOG.info("Not pruning invalid list since the min prune upper bound {} is greater than the min invalid id {}",
                 minPruneUpperBound, invalids[0]);
        return;
      }

      LOG.debug("Removing the following invalid ids from the invalid list", toTruncate);
      txManager.truncateInvalidTx(toTruncate);
      LOG.info("Removed {} invalid ids from the invalid list", toTruncate.size());

      // Call prune complete on all plugins
      Long maxPrunedInvalid = toTruncate.last();
      for (Map.Entry<String, TransactionPruningPlugin> entry : plugins.entrySet()) {
        String name = entry.getKey();
        TransactionPruningPlugin plugin = entry.getValue();
        try {
          LOG.debug("Calling prune complete on plugin {}", name);
          plugin.pruneComplete(now, maxPrunedInvalid);
        } catch (Exception e) {
          // Ignore any exceptions and continue with other plugins
          LOG.error("Got error while calling prune complete on plugin {}", name, e);
        }
      }

      LOG.info("Invalid prune run for time {} is complete", now);
    } catch (Exception e) {
      LOG.error("Got exception during invalid list prune run", e);
    }
  }

  @VisibleForTesting
  long getTime() {
    return System.currentTimeMillis();
  }
}
