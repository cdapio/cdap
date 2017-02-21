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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Objects;
import java.util.SortedSet;

/**
 * Contains information on the set of transactional regions recorded at a given time
 */
@SuppressWarnings("WeakerAccess")
public class TimeRegions {
  static final Function<byte[], String> BYTE_ARR_TO_STRING_FN =
    new Function<byte[], String>() {
      @Override
      public String apply(byte[] input) {
        return Bytes.toStringBinary(input);
      }
    };

  private final long time;
  private final SortedSet<byte[]> regions;

  public TimeRegions(long time, SortedSet<byte[]> regions) {
    this.time = time;
    this.regions = regions;
  }

  public long getTime() {
    return time;
  }

  public SortedSet<byte[]> getRegions() {
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
    TimeRegions that = (TimeRegions) o;
    return time == that.time &&
      Objects.equals(regions, that.regions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, regions);
  }

  @Override
  public String toString() {
    Iterable<String> regionStrings = Iterables.transform(regions, BYTE_ARR_TO_STRING_FN);
    return "TimeRegions{" +
      "time=" + time +
      ", regions=[" + Joiner.on(" ").join(regionStrings) + "]" +
      '}';
  }
}
