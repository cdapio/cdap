/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.util;

import com.google.common.primitives.Longs;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// TODO: Most of this class is copied from {@link co.cask.cdap.common.app.RunIds}. Need to reduce code duplication.
/**
 * Generates an unique ID for a report using type 1 and variant 2 time-based {@link UUID}.
 * This implements time-based UUID generation algorithm described in
 * <a href="http://www.ietf.org/rfc/rfc4122.txt">A Universally Unique IDentifier (UUID) URN Namespace</a>
 * with the following modifications:
 * <ul>
 *   <li>It does not share state with other instances of time-based UUID generators on a given machine. So it is
 *   recommended not to run more than one instance of this on a machine to guarantee uniqueness of UUIDs generated.</li>
 *   <li>The timestamp embedded in the UUID is only valid up to millisecond precision. This is because this
 *   implementation uses a counter value in the remaining bits to ensure unique UUIDs are generated when invoked
 *   multiple times at the same millisecond (up to 10000 times).</li>
 * </ul>
 */
public final class ReportIds {
  private static final Random RANDOM = new Random();

  // Number of 100ns intervals since 15 October 1582 00:00:000000000 until UNIX epoch
  private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
  // Multiplier to convert millisecond into 100ns
  private static final long HUNDRED_NANO_MULTIPLIER = 10000;

  private static final AtomicLong COUNTER = new AtomicLong();


  /**
   * @return UUID based on current time. If called repeatedly within the same millisecond, this is
   * guaranteed to generate at least 10000 unique UUIDs for the millisecond.
   */
  public static UUID generate() {
    return (generateUUIDForTime(System.currentTimeMillis()));
  }

  /**
   * Converts string representation of run id into {@link UUID}l
   */
  public static UUID fromString(String id) {
    return UUID.fromString(id);
  }

  /**
   * @return time from the UUID if it is a time-based UUID, -1 otherwise.
   */
  public static long getTime(String reportId, TimeUnit timeUnit) {
    UUID uuid = UUID.fromString(reportId);
    if (uuid.version() == 1 && uuid.variant() == 2) {
      long timeInMilliseconds = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / HUNDRED_NANO_MULTIPLIER;
      return timeUnit.convert(timeInMilliseconds, TimeUnit.MILLISECONDS);
    }
    return -1;
  }

  private static UUID generateUUIDForTime(long timeInMillis) {
    // Use system time in milliseconds to generate time in 100ns.
    // Use COUNTER to ensure unique time gets generated for the same millisecond (up to HUNDRED_NANO_MULTIPLIER)
    // Hence the time is valid only for millisecond precision, event though it represents 100ns precision.
    long ts = timeInMillis * HUNDRED_NANO_MULTIPLIER + COUNTER.incrementAndGet() % HUNDRED_NANO_MULTIPLIER;
    long time = ts + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

    long timeLow = time &       0xffffffffL;
    long timeMid = time &   0xffff00000000L;
    long timeHi = time & 0xfff000000000000L;
    long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48);

    // Random clock ID
    int clockId = RANDOM.nextInt() & 0x3FFF;
    long nodeId;

    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      NetworkInterface networkInterface = null;
      while (interfaces.hasMoreElements()) {
        networkInterface = interfaces.nextElement();
        if (!networkInterface.isLoopback()) {
          break;
        }
      }
      byte[] mac = networkInterface == null ? null : networkInterface.getHardwareAddress();
      if (mac == null) {
        nodeId = (RANDOM.nextLong() & 0xFFFFFFL) | 0x100000L;
      } else {
        nodeId = Longs.fromBytes((byte) 0, (byte) 0, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
      }

    } catch (SocketException e) {
      // Generate random node ID
      nodeId = RANDOM.nextLong() & 0xFFFFFFL | 0x100000L;
    }

    long lowerLong = ((long) clockId | 0x8000) << 48 | nodeId;

    return new java.util.UUID(upperLong, lowerLong);
  }
}
