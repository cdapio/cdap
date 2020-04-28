/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.common;

import com.google.common.net.InetAddresses;

import java.net.InetAddress;

/**
 * Represents an IP range with a lower and upper bound.
 */
public final class IPRange {

  private final InetAddress lower;
  private final InetAddress upper;

  IPRange(String cidrBlock) {
    String[] parts = cidrBlock.split("/");
    int size = parts.length == 1 ? 0 : Integer.parseInt(parts[1]);
    this.lower = InetAddresses.forString(parts[0]);
    this.upper = InetAddresses.fromInteger(InetAddresses.coerceToInteger(lower) + (1 << (32 - size)) - 1);
  }

  /**
   * Returns {@code true} if the given {@link IPRange} overlaps with this IP range.
   */
  public boolean isOverlap(IPRange range) {
    int ourLower = InetAddresses.coerceToInteger(lower);
    int ourUpper = InetAddresses.coerceToInteger(upper);
    int theirLower = InetAddresses.coerceToInteger(range.lower);
    int theirUpper = InetAddresses.coerceToInteger(range.upper);

    return ourLower <= theirUpper && theirLower <= ourUpper;
  }

  @Override
  public String toString() {
    return "IPRange{" +
      "lower=" + lower +
      ", upper=" + upper +
      '}';
  }
}
