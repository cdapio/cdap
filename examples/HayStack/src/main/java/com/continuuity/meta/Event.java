package com.continuuity.meta;

import java.util.Map;

/**
 * Defines an Event in the system.
 */
public class Event {

  private long timestamp;

  private String payload;

  private Map<String, String> dimensions;

  public Event(long timestamp, String payload, Map<String, String> dimensions) {
    this.timestamp = timestamp;
    this.payload = payload;
    this.dimensions = dimensions;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getPayload() {
    return payload;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  @Override
  public String toString() {
    return "Event{" +
      "timestamp=" + timestamp +
      ", payload='" + payload + '\'' +
      ", dimensions=" + dimensions +
      '}';
  }
}
