package com.continuuity.testsuite.purchaseanalytics.datamodel;

import java.util.UUID;

/**
 * Base class generating a uuid at creation. Unused for rev 1.0 because of serialization problem with UUIDs.
 * Will replace object specific Ids later on.
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
