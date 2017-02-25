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

import com.google.common.collect.Iterables;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * Invalid List Pruning Debug Tool.
 */
public class InvalidListPruningDebug {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidListPruningDebug.class);
  private static final Gson GSON = new Gson();
  private DataJanitorState dataJanitorState;
  private HConnection connection;
  private TableName tableName;

  /**
   * Initialize the Invalid List Debug Tool.
   * @param conf {@link Configuration}
   * @throws IOException
   */
  public void initialize(final Configuration conf) throws IOException {
    LOG.debug("InvalidListPruningDebugMain : initialize method called");
    connection = new HBaseAdmin(conf).getConnection();
    tableName = TableName.valueOf(conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                           TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE));
    dataJanitorState = new DataJanitorState(new DataJanitorState.TableSupplier() {
      @Override
      public HTableInterface get() throws IOException {
        return connection.getTable(tableName);
      }
    });
  }

  public void destroy() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Returns a set of regions that are live but are not empty nor have a prune upper bound recorded. These regions
   * will stop the progress of pruning.
   *
   * @return {@link Set} of regions that needs to be compacted and flushed
   */
  public Set<String> getRegionsToBeCompacted() throws IOException {
    // Fetch the live regions
    Map<Long, SortedSet<String>> latestTimeRegion = getRegionsOnOrBeforeTime(System.currentTimeMillis());
    if (latestTimeRegion.isEmpty()) {
      return new HashSet<>();
    }

    Long timestamp = latestTimeRegion.keySet().iterator().next();
    SortedSet<String> liveRegions = latestTimeRegion.get(timestamp);

    SortedSet<byte[]> emptyRegions = dataJanitorState.getEmptyRegionsAfterTime(timestamp, null);
    SortedSet<String> emptyRegionNames = new TreeSet<>();
    Iterable<String> regionStrings = Iterables.transform(emptyRegions, TimeRegions.BYTE_ARR_TO_STRING_FN);
    for (String regionString : regionStrings) {
      emptyRegionNames.add(regionString);
    }

    Set<String> nonEmptyRegions = Sets.newHashSet(Sets.difference(liveRegions, emptyRegionNames));

    // Get all pruned regions and remove them from the nonEmptyRegions, resulting in a set of regions that are
    // not empty and have not been registered prune upper bound
    Queue<RegionPruneInfo> prunedRegions = getIdleRegions(-1);
    for (RegionPruneInfo prunedRegion : prunedRegions) {
      if (nonEmptyRegions.contains(prunedRegion.getRegionNameAsString())) {
        nonEmptyRegions.remove(prunedRegion.getRegionNameAsString());
      }
    }

    return nonEmptyRegions;
  }

  /**
   * Return a list of RegionPruneInfo. These regions are the ones that have the lowest prune upper bounds.
   * If -1 is passed in, all the regions and their prune upper bound will be returned. Note that only the regions
   * that are known to be live will be returned.
   *
   * @param numRegions number of regions
   * @return Map of region name and its prune upper bound
   */
  public Queue<RegionPruneInfo> getIdleRegions(Integer numRegions) throws IOException {
    List<RegionPruneInfo> regionPruneInfos = dataJanitorState.getPruneInfoForRegions(null);
    if (regionPruneInfos.isEmpty()) {
      return new LinkedList<>();
    }

    // Create a set with region names
    Set<String> pruneRegionNameSet = new HashSet<>();
    for (RegionPruneInfo regionPruneInfo : regionPruneInfos) {
      pruneRegionNameSet.add(regionPruneInfo.getRegionNameAsString());
    }

    // Fetch the live regions
    Map<Long, SortedSet<String>> latestTimeRegion = getRegionsOnOrBeforeTime(System.currentTimeMillis());
    if (!latestTimeRegion.isEmpty()) {
      SortedSet<String> liveRegions = latestTimeRegion.values().iterator().next();
      Set<String> liveRegionsWithPruneInfo = Sets.intersection(liveRegions, pruneRegionNameSet);
      List<RegionPruneInfo> liveRegionWithPruneInfoList = new ArrayList<>();
      for (RegionPruneInfo regionPruneInfo : regionPruneInfos) {
        if (liveRegionsWithPruneInfo.contains(regionPruneInfo.getRegionNameAsString())) {
          liveRegionWithPruneInfoList.add(regionPruneInfo);
        }
      }

      // Use the subset of live regions and prune regions
      regionPruneInfos = liveRegionWithPruneInfoList;
    }

    if (numRegions < 0) {
      numRegions = regionPruneInfos.size();
    }

    Queue<RegionPruneInfo> lowestPrunes = MinMaxPriorityQueue.orderedBy(new Comparator<RegionPruneInfo>() {
      @Override
      public int compare(RegionPruneInfo o1, RegionPruneInfo o2) {
        return (int) (o1.getPruneUpperBound() - o2.getPruneUpperBound());
      }
    }).maximumSize(numRegions).create();

    for (RegionPruneInfo pruneInfo : regionPruneInfos) {
      lowestPrunes.add(pruneInfo);
    }
    return lowestPrunes;
  }

  /**
   * Return the prune upper bound value of a given region. If no prune upper bound has been written for this region yet,
   * it will return a null.
   *
   * @param regionId region id
   * @return {@link RegionPruneInfo} of the region
   * @throws IOException if there are any errors while trying to fetch the {@link RegionPruneInfo}
   */
  @Nullable
  public RegionPruneInfo getRegionPruneInfo(String regionId) throws IOException {
    return dataJanitorState.getPruneInfoForRegion(Bytes.toBytesBinary(regionId));
  }

  /**
   *
   * @param time Given a time, provide the {@link TimeRegions} at or before that time
   * @return transactional regions that are present at or before the given time
   * @throws IOException if there are any errors while trying to fetch the {@link TimeRegions}
   */
  public Map<Long, SortedSet<String>> getRegionsOnOrBeforeTime(Long time) throws IOException {
    Map<Long, SortedSet<String>> regionMap = new HashMap<>();
    TimeRegions timeRegions = dataJanitorState.getRegionsOnOrBeforeTime(time);
    if (timeRegions == null) {
      return regionMap;
    }
    SortedSet<String> regionNames = new TreeSet<>();
    Iterable<String> regionStrings = Iterables.transform(timeRegions.getRegions(), TimeRegions.BYTE_ARR_TO_STRING_FN);
    for (String regionString : regionStrings) {
      regionNames.add(regionString);
    }
    regionMap.put(timeRegions.getTime(), regionNames);
    return regionMap;
  }

  private void printUsage(PrintWriter pw) {
    pw.println("Usage : org.apache.tephra.hbase.txprune.InvalidListPruning <command> <parameter>");
    pw.println("Available commands, corresponding parameters are:");
    pw.println("****************************************************");
    pw.println("time-region ts");
    pw.println("Desc: Prints out the transactional regions present in HBase at time 'ts' (in milliseconds) " +
                 "or the latest time before time 'ts'.");
    pw.println("idle-regions limit");
    pw.println("Desc: Prints out 'limit' number of regions which has the lowest prune upper bounds. If '-1' is " +
                 "provided as the limit, prune upper bounds of all regions are returned.");
    pw.println("prune-info region-name-as-string");
    pw.println("Desc: Prints out the Pruning information for the region 'region-name-as-string'");
  }

  private boolean execute(String[] args) throws IOException {
    try (PrintWriter pw = new PrintWriter(System.out)) {
      if (args.length != 2) {
        printUsage(pw);
        return false;
      }

      String command = args[0];
      String parameter = args[1];
      if ("time-region".equals(command)) {
        Long time = Long.parseLong(parameter);
        Map<Long, SortedSet<String>> timeRegion = getRegionsOnOrBeforeTime(time);
        pw.println(GSON.toJson(timeRegion));
        return true;
      } else if ("idle-regions".equals(command)) {
        Integer numRegions = Integer.parseInt(parameter);
        Queue<RegionPruneInfo> regionPruneInfos = getIdleRegions(numRegions);
        pw.println(GSON.toJson(regionPruneInfos));
        return true;
      } else if ("prune-info".equals(command)) {
        RegionPruneInfo regionPruneInfo = getRegionPruneInfo(parameter);
        if (regionPruneInfo != null) {
          pw.println(GSON.toJson(regionPruneInfo));
        } else {
          pw.println(String.format("No prune info found for the region %s.", parameter));
        }
        return true;
      } else {
        pw.println(String.format("%s is not a valid command.", command));
        printUsage(pw);
        return false;
      }
    }
  }

  public static void main(String[] args) {
    Configuration hConf = HBaseConfiguration.create();
    InvalidListPruningDebug pruningDebug = new InvalidListPruningDebug();
    try {
      pruningDebug.initialize(hConf);
      boolean success = pruningDebug.execute(args);
      pruningDebug.destroy();
      if (!success) {
        System.exit(1);
      }
    } catch (IOException ex) {
      LOG.error("Received an exception while trying to execute the debug tool. ", ex);
    }
  }
}
