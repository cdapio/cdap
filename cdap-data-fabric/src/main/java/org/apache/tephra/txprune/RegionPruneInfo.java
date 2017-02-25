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

import java.util.Objects;

/**
 * Contains the region id, prune upper bound and prune record timestamp information.
 */
public class RegionPruneInfo {
  private transient byte[] regionName;
  private final String regionNameAsString;
  private final long pruneUpperBound;
  private final long pruneRecordTime;

  public RegionPruneInfo(byte[] regionName, String regionNameAsString, long pruneUpperBound, long pruneRecordTime) {
    this.regionName = regionName;
    this.regionNameAsString = regionNameAsString;
    this.pruneUpperBound = pruneUpperBound;
    this.pruneRecordTime = pruneRecordTime;
  }

  public byte[] getRegionName() {
    return regionName;
  }

  public String getRegionNameAsString() {
    return regionNameAsString;
  }

  public long getPruneUpperBound() {
    return pruneUpperBound;
  }

  public long getPruneRecordTime() {
    return pruneRecordTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionName, regionNameAsString, pruneUpperBound, pruneRecordTime);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    RegionPruneInfo other = (RegionPruneInfo) obj;
    return Objects.equals(regionName, other.getRegionName())
      && Objects.equals(regionNameAsString, other.getRegionNameAsString())
      && Objects.equals(pruneUpperBound, other.getPruneUpperBound())
      && Objects.equals(pruneRecordTime, other.getPruneRecordTime());
  }

  @Override
  public String toString() {
    return "RegionPruneInfo{" +
      "regionNameAsString='" + regionNameAsString + '\'' +
      ", pruneUpperBound='" + pruneUpperBound + '\'' +
      ", pruneRecordTime=" + pruneRecordTime +
      '}';
  }
}
