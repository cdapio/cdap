package com.continuuity.testsuite.purchaseanalytics.datamodel;

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
