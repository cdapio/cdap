package com.continuuity.metadata.types;

import com.google.common.base.Objects;

/**
 * Represents an application.
 */
public class Application {
  private final String id;
  private String name;
  private String description;

  public Application(String id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Application that = (Application) o;
    return Objects.equal(id, that.id) && Objects.equal(name, that.name) && Objects.equal(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, description);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("name", name)
                  .add("description", description)
                  .toString();
  }
}
