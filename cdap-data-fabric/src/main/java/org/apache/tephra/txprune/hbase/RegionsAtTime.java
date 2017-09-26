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

import java.text.DateFormat;
import java.util.Objects;
import java.util.SortedSet;

/**
 * Represents the regions recorded at give time.
 */
@SuppressWarnings("WeakerAccess")
public class RegionsAtTime {
  private final long time;
  private final String timeAsString;
  private final SortedSet<String> regions;

  public RegionsAtTime(long time, SortedSet<String> regions, DateFormat dateFormat) {
    this.time = time;
    this.timeAsString = dateFormat.format(time);
    this.regions = regions;
  }

  public long getTime() {
    return time;
  }

  public String getTimeAsString() {
    return timeAsString;
  }

  public SortedSet<String> getRegions() {
    return regions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegionsAtTime that = (RegionsAtTime) o;
    return time == that.time &&
      Objects.equals(timeAsString, that.timeAsString) &&
      Objects.equals(regions, that.regions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, timeAsString, regions);
  }

  @Override
  public String toString() {
    return "RegionsAtTime{" +
      "time=" + time +
      ", timeAsString='" + getTimeAsString() + '\'' +
      ", regions=" + getRegions() +
      '}';
  }
}
