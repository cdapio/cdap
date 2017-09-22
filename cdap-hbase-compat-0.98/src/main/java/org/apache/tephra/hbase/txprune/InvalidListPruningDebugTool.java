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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.apache.tephra.txprune.hbase.InvalidListPruningDebug;
import org.apache.tephra.txprune.hbase.RegionsAtTime;
import org.apache.tephra.util.TimeMathParser;
import org.apache.tephra.util.TxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Invalid List Pruning Debug Tool.
 */
public class InvalidListPruningDebugTool implements InvalidListPruningDebug {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidListPruningDebugTool.class);
  private static final Gson GSON = new Gson();
  private static final String NOW = "now";
  @VisibleForTesting
  static final String DATE_FORMAT = "d-MMM-yyyy HH:mm:ss z";

  private DataJanitorState dataJanitorState;
  private HConnection connection;
  private TableName tableName;

  /**
   * Initialize the Invalid List Debug Tool.
   * @param conf {@link Configuration}
   * @throws IOException when not able to create an HBase connection
   */
  @Override
  @SuppressWarnings("WeakerAccess")
  public void initialize(final Configuration conf) throws IOException {
    LOG.debug("InvalidListPruningDebugMain : initialize method called");
    connection = HConnectionManager.createConnection(conf);
    tableName = TableName.valueOf(conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                           TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE));
    dataJanitorState = new DataJanitorState(new DataJanitorState.TableSupplier() {
      @Override
      public HTableInterface get() throws IOException {
        return connection.getTable(tableName);
      }
    });
  }

  @Override
  @SuppressWarnings("WeakerAccess")
  public void destroy() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Returns a set of regions that are live but are not empty nor have a prune upper bound recorded. These regions
   * will stop the progress of pruning.
   * <p/>
   * Note that this can return false positives in the following case -
   * At time 't' empty regions were recorded, and time 't+1' prune iteration was invoked.
   * Since  a new set of regions was recorded at time 't+1', all regions recorded as empty before time 't + 1' will
   * now be reported as blocking the pruning, even though they are empty. This is because we cannot tell if those
   * regions got any new data between time 't' and 't + 1'.
   *
   * @param numRegions number of regions
   * @param time time in milliseconds or relative time, regions recorded before the given time are returned
   * @return {@link Set} of regions that needs to be compacted and flushed
   */
  @Override
  @SuppressWarnings("WeakerAccess")
  public Set<String> getRegionsToBeCompacted(Integer numRegions, String time) throws IOException {
    // Fetch the live regions at the given time
    RegionsAtTime timeRegion = getRegionsOnOrBeforeTime(time);
    if (timeRegion.getRegions().isEmpty()) {
      return Collections.emptySet();
    }

    Long timestamp = timeRegion.getTime();
    SortedSet<String> regions = timeRegion.getRegions();

    // Get the live regions
    SortedSet<String> liveRegions = getRegionsOnOrBeforeTime(NOW).getRegions();
    // Retain only the live regions
    regions = Sets.newTreeSet(Sets.intersection(liveRegions, regions));

    SortedSet<byte[]> emptyRegions = dataJanitorState.getEmptyRegionsAfterTime(timestamp, null);
    SortedSet<String> emptyRegionNames = new TreeSet<>();
    Iterable<String> regionStrings = Iterables.transform(emptyRegions, TimeRegions.BYTE_ARR_TO_STRING_FN);
    for (String regionString : regionStrings) {
      emptyRegionNames.add(regionString);
    }

    Set<String> nonEmptyRegions = Sets.newHashSet(Sets.difference(regions, emptyRegionNames));

    // Get all pruned regions for the current time and remove them from the nonEmptyRegions,
    // resulting in a set of regions that are not empty and have not been registered prune upper bound
    List<RegionPruneInfo> prunedRegions = dataJanitorState.getPruneInfoForRegions(null);
    for (RegionPruneInfo prunedRegion : prunedRegions) {
      if (nonEmptyRegions.contains(prunedRegion.getRegionNameAsString())) {
        nonEmptyRegions.remove(prunedRegion.getRegionNameAsString());
      }
    }

    if ((numRegions < 0) || (numRegions >= nonEmptyRegions.size())) {
      return nonEmptyRegions;
    }

    Set<String> subsetRegions = new HashSet<>(numRegions);
    for (String regionName : nonEmptyRegions) {
      if (subsetRegions.size() == numRegions) {
        break;
      }
      subsetRegions.add(regionName);
    }
    return subsetRegions;
  }

  /**
   * Return a list of RegionPruneInfo. These regions are the ones that have the lowest prune upper bounds.
   * If -1 is passed in, all the regions and their prune upper bound will be returned. Note that only the regions
   * that are known to be live will be returned.
   *
   * @param numRegions number of regions
   * @param time time in milliseconds or relative time, regions recorded before the given time are returned
   * @return Map of region name and its prune upper bound
   */
  @Override
  @SuppressWarnings("WeakerAccess")
  public SortedSet<RegionPruneInfoPretty> getIdleRegions(Integer numRegions, String time) throws IOException {
    List<RegionPruneInfo> regionPruneInfos = dataJanitorState.getPruneInfoForRegions(null);
    if (regionPruneInfos.isEmpty()) {
      return new TreeSet<>();
    }

    // Create a set with region names
    Set<String> pruneRegionNameSet = new HashSet<>();
    for (RegionPruneInfo regionPruneInfo : regionPruneInfos) {
      pruneRegionNameSet.add(regionPruneInfo.getRegionNameAsString());
    }

    // Fetch the latest live regions
    RegionsAtTime latestRegions = getRegionsOnOrBeforeTime(NOW);

    // Fetch the regions at the given time
    RegionsAtTime timeRegions = getRegionsOnOrBeforeTime(time);
    Set<String> liveRegions = Sets.intersection(latestRegions.getRegions(), timeRegions.getRegions());
    Set<String> liveRegionsWithPruneInfo = Sets.intersection(liveRegions, pruneRegionNameSet);
    List<RegionPruneInfo> liveRegionWithPruneInfoList = new ArrayList<>();
    for (RegionPruneInfo regionPruneInfo : regionPruneInfos) {
      if (liveRegionsWithPruneInfo.contains(regionPruneInfo.getRegionNameAsString())) {
        liveRegionWithPruneInfoList.add(regionPruneInfo);
      }

      // Use the subset of live regions and prune regions
      regionPruneInfos = liveRegionWithPruneInfoList;
    }

    if (numRegions < 0) {
      numRegions = regionPruneInfos.size();
    }

    Comparator<RegionPruneInfo> comparator = new Comparator<RegionPruneInfo>() {
      @Override
      public int compare(RegionPruneInfo o1, RegionPruneInfo o2) {
        int result = Long.compare(o1.getPruneUpperBound(), o2.getPruneUpperBound());
        if (result == 0) {
          return o1.getRegionNameAsString().compareTo(o2.getRegionNameAsString());
        }
        return result;
      }
    };
    MinMaxPriorityQueue<RegionPruneInfoPretty> lowestPrunes =
      MinMaxPriorityQueue.orderedBy(comparator).maximumSize(numRegions).create();

    for (RegionPruneInfo pruneInfo : regionPruneInfos) {
      lowestPrunes.add(new RegionPruneInfoPretty(pruneInfo));
    }

    SortedSet<RegionPruneInfoPretty> regions = new TreeSet<>(comparator);
    regions.addAll(lowestPrunes);
    return regions;
  }

  /**
   * Return the prune upper bound value of a given region. If no prune upper bound has been written for this region yet,
   * it will return a null.
   *
   * @param regionId region id
   * @return {@link RegionPruneInfo} of the region
   * @throws IOException if there are any errors while trying to fetch the {@link RegionPruneInfo}
   */
  @Override
  @SuppressWarnings("WeakerAccess")
  @Nullable
  public RegionPruneInfoPretty getRegionPruneInfo(String regionId) throws IOException {
    RegionPruneInfo pruneInfo = dataJanitorState.getPruneInfoForRegion(Bytes.toBytesBinary(regionId));
    return pruneInfo == null ? null : new RegionPruneInfoPretty(pruneInfo);
  }

  /**
   *
   * @param timeString Given a time, provide the {@link TimeRegions} at or before that time.
   *                   Time can be in milliseconds or relative time.
   * @return transactional regions that are present at or before the given time
   * @throws IOException if there are any errors while trying to fetch the {@link TimeRegions}
   */
  @Override
  @SuppressWarnings("WeakerAccess")
  public RegionsAtTime getRegionsOnOrBeforeTime(String timeString) throws IOException {
    long time = TimeMathParser.parseTime(timeString, TimeUnit.MILLISECONDS);
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    TimeRegions timeRegions = dataJanitorState.getRegionsOnOrBeforeTime(time);
    if (timeRegions == null) {
      return new RegionsAtTime(time, new TreeSet<String>(), dateFormat);
    }
    SortedSet<String> regionNames = new TreeSet<>();
    Iterable<String> regionStrings = Iterables.transform(timeRegions.getRegions(), TimeRegions.BYTE_ARR_TO_STRING_FN);
    for (String regionString : regionStrings) {
      regionNames.add(regionString);
    }
    return new RegionsAtTime(timeRegions.getTime(), regionNames, dateFormat);
  }

  private void printUsage(PrintWriter pw) {
    pw.println();
    pw.println("Usage : org.apache.tephra.hbase.txprune.InvalidListPruning <command> <parameters>");
    pw.println();
    pw.println("Available commands");
    pw.println("------------------");
    pw.println("to-compact-regions limit [time]");
    pw.println("Desc: Prints out the regions that are active, but not empty, " +
                 "and have not registered a prune upper bound.");
    pw.println();
    pw.println("idle-regions limit [time]");
    pw.println("Desc: Prints out the regions that have the lowest prune upper bounds.");
    pw.println();
    pw.println("prune-info region-name-as-string");
    pw.println("Desc: Prints the prune upper bound and the time it was recorded for the given region.");
    pw.println();
    pw.println("time-region [time]");
    pw.println("Desc: Prints out the transactional regions present in HBase recorded at or before the given time.");
    pw.println();
    pw.println("Parameters");
    pw.println("----------");
    pw.println(" * limit - used to limit the number of regions returned, -1 to apply no limit");
    pw.println(" * time  - if time is not provided, the current time is used. ");
    pw.println("             When provided, the data recorded on or before the given time is returned.");
    pw.println("             Time can be provided in milliseconds, or can be provided as a relative time.");
    pw.println("             Examples for relative time -");
    pw.println("             now = current time,");
    pw.println("             now-1d = current time - 1 day,");
    pw.println("             now-1d+4h = 20 hours before now,");
    pw.println("             now+5s = current time + 5 seconds");
    pw.println();
  }

  @VisibleForTesting
  boolean execute(String[] args, PrintWriter out) throws IOException {
      if (args.length < 1) {
        printUsage(out);
        return false;
      }

    String command = args[0];
    switch (command) {
        case "time-region":
          if (args.length <= 2) {
            String time = args.length == 2 ? args[1] : NOW;
            RegionsAtTime timeRegion = getRegionsOnOrBeforeTime(time);
            out.println(GSON.toJson(timeRegion));
            return true;
          }
          break;
        case "idle-regions":
          if (args.length <= 3) {
            Integer numRegions = Integer.parseInt(args[1]);
            String time = args.length == 3 ? args[2] : NOW;
            SortedSet<RegionPruneInfoPretty> regionPruneInfos = getIdleRegions(numRegions, time);
            out.println(GSON.toJson(regionPruneInfos));
            return true;
          }
          break;
        case "prune-info":
          if (args.length == 2) {
            String regionName = args[1];
            RegionPruneInfo regionPruneInfo = getRegionPruneInfo(regionName);
            if (regionPruneInfo != null) {
              out.println(GSON.toJson(regionPruneInfo));
            } else {
              out.println(String.format("No prune info found for the region %s.", regionName));
            }
            return true;
          }
          break;
        case "to-compact-regions":
          if (args.length <= 3) {
            Integer numRegions = Integer.parseInt(args[1]);
            String time = args.length == 3 ? args[2] : NOW;
            Set<String> toBeCompactedRegions = getRegionsToBeCompacted(numRegions, time);
            out.println(GSON.toJson(toBeCompactedRegions));
            return true;
          }
          break;
      }

      printUsage(out);
      return false;
  }

  public static void main(String[] args) {
    Configuration hConf = HBaseConfiguration.create();
    InvalidListPruningDebugTool pruningDebug = new InvalidListPruningDebugTool();
    try (PrintWriter out = new PrintWriter(System.out)) {
      pruningDebug.initialize(hConf);
      boolean success = pruningDebug.execute(args, out);
      pruningDebug.destroy();
      if (!success) {
        System.exit(1);
      }
    } catch (IOException ex) {
      LOG.error("Received an exception while trying to execute the debug tool. ", ex);
    }
  }

  /**
   * Wrapper class around {@link RegionPruneInfo} to print human readable dates for timestamps.
   */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class RegionPruneInfoPretty extends RegionPruneInfo {
    private final transient SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    private final String pruneUpperBoundAsString;
    private final String pruneRecordTimeAsString;

    public RegionPruneInfoPretty(RegionPruneInfo regionPruneInfo) {
      this(regionPruneInfo.getRegionName(), regionPruneInfo.getRegionNameAsString(),
           regionPruneInfo.getPruneUpperBound(), regionPruneInfo.getPruneRecordTime());
    }

    public RegionPruneInfoPretty(byte[] regionName, String regionNameAsString,
                                 long pruneUpperBound, long pruneRecordTime) {
      super(regionName, regionNameAsString, pruneUpperBound, pruneRecordTime);
      pruneUpperBoundAsString = dateFormat.format(TxUtils.getTimestamp(pruneUpperBound));
      pruneRecordTimeAsString = dateFormat.format(pruneRecordTime);
    }

    public String getPruneUpperBoundAsString() {
      return pruneUpperBoundAsString;
    }

    public String getPruneRecordTimeAsString() {
      return pruneRecordTimeAsString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      RegionPruneInfoPretty that = (RegionPruneInfoPretty) o;
      return Objects.equals(pruneUpperBoundAsString, that.pruneUpperBoundAsString) &&
        Objects.equals(pruneRecordTimeAsString, that.pruneRecordTimeAsString);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), pruneUpperBoundAsString, pruneRecordTimeAsString);
    }

    @Override
    public String toString() {
      return "RegionPruneInfoPretty{" +
        ", pruneUpperBoundAsString='" + pruneUpperBoundAsString + '\'' +
        ", pruneRecordTimeAsString='" + pruneRecordTimeAsString + '\'' +
        "} " + super.toString();
    }
  }

}
