package com.continuuity.internal.app.services.legacy;

import com.google.common.base.Objects;

/**
 * Defines the types of flowlets
 */
public enum FlowletType {
  SOURCE(1, "Source"),
  COMPUTE(2, "Compute"),
  SINK(3, "Sink");

  private int id;
  private String name;

  FlowletType(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("name", name)
      .toString();
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }


}
