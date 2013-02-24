package com.continuuity.app.runtime;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents an unique ID of a running program.
 */
public final class RunId {

  private static final AtomicLong lastTimestamp = new AtomicLong();

  private final UUID id;

  public static RunId generate() {
    return new RunId(generateUUIDFromCurrentTime());
  }

  public static RunId from(String id) {
    return new RunId(UUID.fromString(id));
  }

  private RunId(UUID id) {
    this.id = id;
  }

  public String getId() {
    return id.toString();
  }

  @Override
  public String toString() {
    return id.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return id.equals(((RunId)o).id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  private static UUID generateUUIDFromCurrentTime() {
    // Number of 100ns since 15 October 1582 00:00:000000000
    final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    // Use a unique timestamp
    long lastTs;
    long ts;
    do {
      lastTs = lastTimestamp.get();
      ts = System.currentTimeMillis();
      if (ts == lastTs) {
        ts++;
      }
    } while (!lastTimestamp.compareAndSet(lastTs, ts));

    long time = ts * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
    long timeLow = time & 0xffffffffL;
    long timeMid = time & 0xffff00000000L;
    long timeHi = time & 0xfff000000000000L;
    long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48) ;

    // Random clock ID
    Random random = new Random();
    int clockId = random.nextInt() & 0x3FFF;
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
        nodeId = (random.nextLong() & 0xFFFFFFL) | 0x100000L;
      } else {
        nodeId = ((long) Ints.fromBytes(mac[0], mac[1], mac[2], mac[3]) << 16)
          | Ints.fromBytes((byte)0, (byte)0, mac[4], mac[5]);
      }

    } catch (SocketException e) {
      // Generate random node ID
      nodeId = random.nextLong() & 0xFFFFFFL | 0x100000L;
    }

    long lowerLong = ((long)clockId | 0x8000) << 48 | nodeId;

    return new java.util.UUID(upperLong, lowerLong);
  }
}
