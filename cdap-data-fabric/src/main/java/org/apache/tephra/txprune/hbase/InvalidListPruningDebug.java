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

package org.apache.tephra.txprune.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.txprune.RegionPruneInfo;

import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import javax.annotation.Nullable;

/**
 * Interface to debug transaction pruning progress.
 */
public interface InvalidListPruningDebug {
  /**
   * Called once at the beginning to initialize the instance.
   *
   * @param conf {@link Configuration}
   * @throws IOException when not able to initialize.
   */
  void initialize(Configuration conf) throws IOException;

  /**
   * Called once at the end to clean up the resources.
   *
   * @throws IOException when not able to clean up resources.
   */
  void destroy() throws IOException;

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
  Set<String> getRegionsToBeCompacted(Integer numRegions, String time) throws IOException;

  /**
   * Return a list of RegionPruneInfo. These regions are the ones that have the lowest prune upper bounds.
   * If -1 is passed in, all the regions and their prune upper bound will be returned. Note that only the regions
   * that are known to be live will be returned.
   *
   * @param numRegions number of regions
   * @param time time in milliseconds or relative time, regions recorded before the given time are returned
   * @return Set of region name and its prune upper bound
   */
  SortedSet<? extends RegionPruneInfo> getIdleRegions(Integer numRegions, String time)
    throws IOException;

  /**
   * Return the prune upper bound value of a given region. If no prune upper bound has been written for this region yet,
   * it will return a null.
   *
   * @param regionId region id
   * @return {@link RegionPruneInfo} of the region
   * @throws IOException if there are any errors while trying to fetch the {@link RegionPruneInfo}
   */
  @Nullable
  RegionPruneInfo getRegionPruneInfo(String regionId) throws IOException;

  /**
   *
   * @param timeString Given a time, provide the regions at or before that time.
   *                   Time can be in milliseconds or relative time.
   * @return transactional regions that are present at or before the given time
   * @throws IOException if there are any errors while trying to fetch the region list
   */
  RegionsAtTime getRegionsOnOrBeforeTime(String timeString) throws IOException;
}
