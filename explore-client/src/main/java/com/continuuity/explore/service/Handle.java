package com.continuuity.explore.service;

import com.google.common.base.Objects;

import java.util.UUID;

/**
 * Represents an operation that is submitted for execution to {@link Explore}.
 */
public class Handle {
  private final String id;

  public static Handle generate() {
    // TODO: make sure handles are unique across multiple instances.
    return new Handle(UUID.randomUUID().toString());
  }

  public static Handle fromId(String id) {
    return new Handle(id);
  }

  private Handle(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Handle that = (Handle) o;

    return Objects.equal(this.id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }
}
