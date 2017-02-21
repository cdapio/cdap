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

package org.apache.tephra.hbase.txprune;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.tephra.txprune.TransactionPruningPlugin;
import org.apache.tephra.util.TxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Default implementation of the {@link TransactionPruningPlugin} for HBase.
 *
 * This plugin determines the prune upper bound for transactional HBase tables that use
 * coprocessor {@link TransactionProcessor}.
 *
 * <h3>State storage:</h3>
 *
 * This plugin expects the TransactionProcessor to save the prune upper bound for invalid transactions
 * after every major compaction of a region. Let's call this <i>(region, prune upper bound)</i>.
 * In addition, the plugin also persists the following information on a run at time <i>t</i>
 * <ul>
 *   <li>
 *     <i>(t, set of regions)</i>: Set of transactional regions at time <i>t</i>.
 *     Transactional regions are regions of the tables that have the coprocessor TransactionProcessor
 *     attached to them.
 *   </li>
 *   <li>
 *     <i>(t, inactive transaction bound)</i>: This is the smallest not in-progress transaction that
 *     will not have writes in any HBase regions that are created after time <i>t</i>.
 *     This value is determined by the Transaction Service based on the transaction state at time <i>t</i>
 *     and passed on to the plugin.
 *   </li>
 * </ul>
 *
 * <h3>Computing prune upper bound:</h3>
 *
 * In a typical HBase instance, there can be a constant change in the number of regions due to region creations,
 * splits and merges. At any given time there can always be a region on which a major compaction has not been run.
 * Since the prune upper bound will get recorded for a region only after a major compaction,
 * using only the latest set of regions we may not be able to find the
 * prune upper bounds for all the current regions. Hence we persist the set of regions that exist at that time
 * of each run of the plugin, and use historical region set for time <i>t</i>, <i>t - 1</i>, etc.
 * to determine the prune upper bound.
 *
 * From the regions saved at time <i>t</i>, <i>t - 1</i>, etc.,
 * the plugin tries to find the latest <i>(t, set of regions)</i> where all regions have been major compacted,
 * i.e, all regions have prune upper bound recorded in <i>(region, prune upper bound)</i>.
 * <br/>
 * If such a set is found for time <i>t1</i>, the prune upper bound returned by the plugin is the minimum of
 * <ul>
 *   <li>Prune upper bounds of regions in set <i>(t1, set of regions)</i></li>
 *   <li>Inactive transaction bound from <i>(t1, inactive transaction bound)</i></li>
 * </ul>
 *
 * <p/>
 * Above, when we find <i>(t1, set of regions)</i>, there may a region that was created after time <i>t1</i>,
 * but has a data write from an invalid transaction that is smaller than the prune upper bounds of all
 * regions in <i>(t1, set of regions)</i>. This is possible because <i>(region, prune upper bound)</i> persisted by
 * TransactionProcessor is always the latest prune upper bound for a region.
 * <br/>
 * However a region created after time <i>t1</i> cannot have writes from an invalid transaction that is smaller than
 * inactive transaction bound at the time the region was created.
 * Since we limit the plugin prune upper bound using <i>(t1, inactive transaction bound)</i>,
 * there should be no invalid transactions smaller than the plugin prune upper bound with writes in any
 * transactional region of this HBase instance.
 *
 * <p/>
 * Note: If your tables uses a transactional coprocessor other than TransactionProcessor,
 * then you may need to write a new plugin to compute prune upper bound for those tables.
 */
@SuppressWarnings("WeakerAccess")
public class HBaseTransactionPruningPlugin implements TransactionPruningPlugin {
  public static final Logger LOG = LoggerFactory.getLogger(HBaseTransactionPruningPlugin.class);

  protected Configuration conf;
  protected Connection connection;
  protected DataJanitorState dataJanitorState;

  @Override
  public void initialize(Configuration conf) throws IOException {
    this.conf = conf;
    this.connection = ConnectionFactory.createConnection(conf);

    final TableName stateTable = TableName.valueOf(conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                                            TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE));
    LOG.info("Initializing plugin with state table {}:{}", stateTable.getNamespaceAsString(),
             stateTable.getNameAsString());
    createPruneTable(stateTable);
    this.dataJanitorState = new DataJanitorState(new DataJanitorState.TableSupplier() {
      @Override
      public Table get() throws IOException {
        return connection.getTable(stateTable);
      }
    });
  }

  /**
   * Determines prune upper bound for the data store as mentioned above.
   */
  @Override
  public long fetchPruneUpperBound(long time, long inactiveTransactionBound) throws IOException {
    LOG.debug("Fetching prune upper bound for time {} and inactive transaction bound {}",
              time, inactiveTransactionBound);
    if (time < 0 || inactiveTransactionBound < 0) {
      return -1;
    }

    // Get all the current transactional regions
    SortedSet<byte[]> transactionalRegions = getTransactionalRegions();
    if (!transactionalRegions.isEmpty()) {
      LOG.debug("Saving {} transactional regions for time {}", transactionalRegions.size(), time);
      dataJanitorState.saveRegionsForTime(time, transactionalRegions);
      // Save inactive transaction bound for time as the final step.
      // We can then use its existence to make sure that the data for a given time is complete or not
      LOG.debug("Saving inactive transaction bound {} for time {}", inactiveTransactionBound, time);
      dataJanitorState.saveInactiveTransactionBoundForTime(time, inactiveTransactionBound);
    }

    return computePruneUpperBound(new TimeRegions(time, transactionalRegions));
  }

  /**
   * After invalid list has been pruned, this cleans up state information that is no longer required.
   * This includes -
   * <ul>
   *   <li>
   *     <i>(region, prune upper bound)</i> - prune upper bound for regions that are older
   *     than maxPrunedInvalid
   *   </li>
   *   <li>
   *     <i>(t, set of regions) - Regions set that were recorded on or before the start time
   *     of maxPrunedInvalid
   *   </li>
   *   <li>
   *     (t, inactive transaction bound) - Smallest not in-progress transaction without any writes in new regions
   *     information recorded on or before the start time of maxPrunedInvalid
   *   </li>
   * </ul>
   */
  @Override
  public void pruneComplete(long time, long maxPrunedInvalid) throws IOException {
    LOG.debug("Prune complete for time {} and prune upper bound {}", time, maxPrunedInvalid);
    if (time < 0 || maxPrunedInvalid < 0) {
      return;
    }

    // Get regions for the current time, so as to not delete the prune upper bounds for them.
    // The prune upper bounds for regions are recorded by TransactionProcessor and the deletion
    // is done by this class. To avoid update/delete race condition, we only delete prune upper
    // bounds for the stale regions.
    TimeRegions regionsToExclude = dataJanitorState.getRegionsOnOrBeforeTime(time);
    if (regionsToExclude != null) {
      LOG.debug("Deleting prune upper bounds smaller than {} for stale regions", maxPrunedInvalid);
      dataJanitorState.deletePruneUpperBounds(maxPrunedInvalid, regionsToExclude.getRegions());
    } else {
      LOG.warn("Cannot find saved regions on or before time {}", time);
    }
    long pruneTime = TxUtils.getTimestamp(maxPrunedInvalid);
    LOG.debug("Deleting regions recorded before time {}", pruneTime);
    dataJanitorState.deleteAllRegionsOnOrBeforeTime(pruneTime);
    LOG.debug("Deleting inactive transaction bounds recorded on or before time {}", pruneTime);
    dataJanitorState.deleteInactiveTransactionBoundsOnOrBeforeTime(pruneTime);
    LOG.debug("Deleting empty regions recorded on or before time {}", pruneTime);
    dataJanitorState.deleteEmptyRegionsOnOrBeforeTime(pruneTime);
  }

  @Override
  public void destroy() {
    LOG.info("Stopping plugin...");
    try {
      connection.close();
    } catch (IOException e) {
      LOG.error("Got exception while closing HBase connection", e);
    }
  }

  /**
   * Create the prune state table given the {@link TableName} if the table doesn't exist already.
   *
   * @param stateTable prune state table name
   */
  protected void createPruneTable(TableName stateTable) throws IOException {
    try (Admin admin = this.connection.getAdmin()) {
      if (admin.tableExists(stateTable)) {
        LOG.debug("Not creating pruneStateTable {}:{} since it already exists.",
                  stateTable.getNamespaceAsString(), stateTable.getNameAsString());
        return;
      }

      HTableDescriptor htd = new HTableDescriptor(stateTable);
      htd.addFamily(new HColumnDescriptor(DataJanitorState.FAMILY).setMaxVersions(1));
      admin.createTable(htd);
      LOG.info("Created pruneTable {}:{}", stateTable.getNamespaceAsString(), stateTable.getNameAsString());
    } catch (TableExistsException ex) {
      // Expected if the prune state table is being created at the same time by another client
      LOG.debug("Not creating pruneStateTable {}:{} since it already exists.",
                stateTable.getNamespaceAsString(), stateTable.getNameAsString(), ex);
    }
  }

  /**
   * Returns whether the table is a transactional table. By default, it is a table is identified as a transactional
   * table if it has a the coprocessor {@link TransactionProcessor} attached to it. Should be overriden if the users
   * attach a different coprocessor.
   *
   * @param tableDescriptor {@link HTableDescriptor} of the table
   * @return true if the table is transactional
   */
  protected boolean isTransactionalTable(HTableDescriptor tableDescriptor) {
    return tableDescriptor.hasCoprocessor(TransactionProcessor.class.getName());
  }

  protected SortedSet<byte[]> getTransactionalRegions() throws IOException {
    SortedSet<byte[]> regions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    try (Admin admin = connection.getAdmin()) {
      HTableDescriptor[] tableDescriptors = admin.listTables();
      LOG.debug("Got {} tables to process", tableDescriptors == null ? 0 : tableDescriptors.length);
      if (tableDescriptors != null) {
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
          if (isTransactionalTable(tableDescriptor)) {
            List<HRegionInfo> tableRegions = admin.getTableRegions(tableDescriptor.getTableName());
            LOG.debug("Regions for table {}: {}", tableDescriptor.getTableName(), tableRegions);
            if (tableRegions != null) {
              for (HRegionInfo region : tableRegions) {
                regions.add(region.getRegionName());
              }
            }
          } else {
            LOG.debug("{} is not a transactional table", tableDescriptor.getTableName());
          }
        }
      }
    }
    return regions;
  }

  /**
   * Try to find the latest set of regions in which all regions have been major compacted, and
   * compute prune upper bound from them. Starting from newest to oldest, this looks into the
   * region set that has been saved periodically, and joins it with the prune upper bound data
   * for a region recorded after a major compaction.
   *
   * @param timeRegions the latest set of regions
   * @return prune upper bound
   * @throws IOException when not able to talk to HBase
   */
  private long computePruneUpperBound(TimeRegions timeRegions) throws IOException {
    do {
      LOG.debug("Computing prune upper bound for {}", timeRegions);
      SortedSet<byte[]> transactionalRegions = timeRegions.getRegions();
      long time = timeRegions.getTime();

      long inactiveTransactionBound = dataJanitorState.getInactiveTransactionBoundForTime(time);
      LOG.debug("Got inactive transaction bound {}", inactiveTransactionBound);
      // If inactiveTransactionBound is not recorded then that means the data is not complete for these regions
      if (inactiveTransactionBound == -1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring regions for time {} as no inactiveTransactionBound was found for that time, " +
                      "and hence the data must be incomplete", time);
        }
        continue;
      }

      // Get the prune upper bounds for all the transactional regions
      Map<byte[], Long> pruneUpperBoundRegions =
        dataJanitorState.getPruneUpperBoundForRegions(transactionalRegions);
      logPruneUpperBoundRegions(pruneUpperBoundRegions);

      // Use inactiveTransactionBound as the prune upper bound for the empty regions since the regions that are
      // recorded as empty after inactiveTransactionBoundTime will not have invalid data
      // for transactions started on or before inactiveTransactionBoundTime
      pruneUpperBoundRegions = handleEmptyRegions(inactiveTransactionBound, transactionalRegions,
                                                  pruneUpperBoundRegions);

      // If prune upper bounds are found for all the transactional regions, then compute the prune upper bound
      // across all regions
      if (!transactionalRegions.isEmpty() &&
        pruneUpperBoundRegions.size() == transactionalRegions.size()) {
        Long minPruneUpperBoundRegions = Collections.min(pruneUpperBoundRegions.values());
        long pruneUpperBound = Math.min(inactiveTransactionBound, minPruneUpperBoundRegions);
        LOG.debug("Found prune upper bound {} for time {}", pruneUpperBound, time);
        return pruneUpperBound;
      } else {
        if (LOG.isDebugEnabled()) {
          Sets.SetView<byte[]> difference =
            Sets.difference(transactionalRegions, pruneUpperBoundRegions.keySet());
          LOG.debug("Ignoring regions for time {} because the following regions did not record a pruneUpperBound: {}",
                    time, Iterables.transform(difference, TimeRegions.BYTE_ARR_TO_STRING_FN));
        }
      }

      timeRegions = dataJanitorState.getRegionsOnOrBeforeTime(time - 1);
    } while (timeRegions != null);
    return -1;
  }

  private Map<byte[], Long> handleEmptyRegions(long inactiveTransactionBound,
                                               SortedSet<byte[]> transactionalRegions,
                                               Map<byte[], Long> pruneUpperBoundRegions) throws IOException {
    long inactiveTransactionBoundTime = TxUtils.getTimestamp(inactiveTransactionBound);
    SortedSet<byte[]> emptyRegions =
      dataJanitorState.getEmptyRegionsAfterTime(inactiveTransactionBoundTime, transactionalRegions);
    LOG.debug("Got empty transactional regions for inactive transaction bound time {}: {}",
              inactiveTransactionBoundTime, Iterables.transform(emptyRegions, TimeRegions.BYTE_ARR_TO_STRING_FN));

    // The regions that are recorded as empty after inactiveTransactionBoundTime will not have invalid data
    // for transactions started before or on inactiveTransactionBoundTime. Hence we can consider the prune upper bound
    // for these empty regions as inactiveTransactionBound
    Map<byte[], Long> pubWithEmptyRegions = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    pubWithEmptyRegions.putAll(pruneUpperBoundRegions);
    for (byte[] emptyRegion : emptyRegions) {
      if (!pruneUpperBoundRegions.containsKey(emptyRegion)) {
        pubWithEmptyRegions.put(emptyRegion, inactiveTransactionBound);
      }
    }
    return Collections.unmodifiableMap(pubWithEmptyRegions);
  }

  private void logPruneUpperBoundRegions(Map<byte[], Long> pruneUpperBoundRegions) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got region - prune upper bound map: {}",
                Iterables.transform(pruneUpperBoundRegions.entrySet(),
                                    new Function<Map.Entry<byte[], Long>, Map.Entry<String, Long>>() {
                                      @Override
                                      public Map.Entry<String, Long> apply(Map.Entry<byte[], Long> input) {
                                        String regionName = TimeRegions.BYTE_ARR_TO_STRING_FN.apply(input.getKey());
                                        return Maps.immutableEntry(regionName, input.getValue());
                                      }
                                    }));
    }
  }
}
