package com.continuuity.metadata.types;

import com.google.common.base.Objects;

/**
 * Represents the meta data for a stream.
 */
public class Stream {

  private final String id;
  private String name;
  private String description;
  private Long capacityInBytes;
  private Long expiryInSeconds;

  public Stream(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Long getCapacityInBytes() {
    return capacityInBytes;
  }

  public void setCapacityInBytes(long capacityInBytes) {
    this.capacityInBytes = capacityInBytes;
  }

  public Long getExpiryInSeconds() {
    return expiryInSeconds;
  }

  public void setExpiryInSeconds(long expiryInSeconds) {
    this.expiryInSeconds = expiryInSeconds;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("name", name)
                  .add("description", description)
                  .add("capacityInBytes", capacityInBytes)
                  .add("expiryInSeconds", expiryInSeconds)
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
    Stream stream = (Stream) o;
    return Objects.equal(id, stream.id)
      && Objects.equal(name, stream.name)
      && Objects.equal(description, stream.description)
      && Objects.equal(capacityInBytes, stream.capacityInBytes)
      && Objects.equal(expiryInSeconds, stream.expiryInSeconds);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, description, capacityInBytes, expiryInSeconds);
  }
}
