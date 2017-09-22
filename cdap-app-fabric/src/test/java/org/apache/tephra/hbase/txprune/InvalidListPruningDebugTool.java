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

package org.apache.tephra.hbase.txprune;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.apache.tephra.txprune.hbase.InvalidListPruningDebug;
import org.apache.tephra.txprune.hbase.RegionsAtTime;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import javax.annotation.Nullable;

/**
 * A mock implementation of {@link InvalidListPruningDebug} for testing.
 */
@SuppressWarnings("unused")
public class InvalidListPruningDebugTool implements InvalidListPruningDebug {
  private static final Comparator<RegionPruneInfo> PRUNE_INFO_COMPARATOR =
    new Comparator<RegionPruneInfo>() {
      @Override
      public int compare(RegionPruneInfo o1, RegionPruneInfo o2) {
        int result = Long.compare(o1.getPruneUpperBound(), o2.getPruneUpperBound());
        if (result == 0) {
          return o1.getRegionNameAsString().compareTo(o2.getRegionNameAsString());
        }
        return result;
      }
    };

  private static Map<String, SortedSet<String>> regionsToBeCompacted;
  private static Map<String, SortedSet<? extends RegionPruneInfo>> idleRegions;
  private static Map<String, RegionPruneInfo> regionPruneInfos;
  private static Map<String, RegionsAtTime> regionsAtTime;

  public static void setRegionsToBeCompacted(Map<String, SortedSet<String>> regionsToBeCompacted) {
    InvalidListPruningDebugTool.regionsToBeCompacted = regionsToBeCompacted;
  }

  public static void setIdleRegions(Map<String, SortedSet<? extends RegionPruneInfo>> idleRegions) {
    InvalidListPruningDebugTool.idleRegions = idleRegions;
  }

  public static void setRegionPruneInfos(Map<String, RegionPruneInfo> regionPruneInfos) {
    InvalidListPruningDebugTool.regionPruneInfos = regionPruneInfos;
  }

  public static void setRegionsAtTime(Map<String, RegionsAtTime> regionsAtTime) {
    InvalidListPruningDebugTool.regionsAtTime = regionsAtTime;
  }

  public static void reset() {
    regionsToBeCompacted = null;
    idleRegions = null;
    regionPruneInfos = null;
    regionsAtTime = null;
  }

  @Override
  public void initialize(Configuration conf) throws IOException {
    // no-op
  }

  @Override
  public void destroy() throws IOException {
    // no-op
  }

  @Override
  public Set<String> getRegionsToBeCompacted(Integer numRegions, String time) throws IOException {
    numRegions = numRegions < 0 ? Integer.MAX_VALUE : numRegions;
    return Sets.newHashSet(Iterables.limit(regionsToBeCompacted.get(time), numRegions));
  }

  @Override
  public SortedSet<? extends RegionPruneInfo> getIdleRegions(Integer numRegions, String time) throws IOException {
    numRegions = numRegions < 0 ? Integer.MAX_VALUE : numRegions;
    SortedSet<? extends RegionPruneInfo> idleRegions = InvalidListPruningDebugTool.idleRegions.get(time);
    if (idleRegions == null) {
      return ImmutableSortedSet.of();
    }
    return ImmutableSortedSet.copyOf(PRUNE_INFO_COMPARATOR, Sets.newHashSet(Iterables.limit(idleRegions, numRegions)));
  }

  @Nullable
  @Override
  public RegionPruneInfo getRegionPruneInfo(String regionId) throws IOException {
    return regionPruneInfos.get(regionId);
  }

  @Override
  public RegionsAtTime getRegionsOnOrBeforeTime(String timeString) throws IOException {
    return regionsAtTime.get(timeString);
  }
}
