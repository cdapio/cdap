package com.continuuity.testsuite.purchaseanalytics;

import java.util.UUID;

/**
 *
 */
public class SerializedObject {
  private final UUID uuid;

  public UUID getUuid() {
    return uuid;
  }

  public SerializedObject() {
    this.uuid = UUID.randomUUID();
  }
}
