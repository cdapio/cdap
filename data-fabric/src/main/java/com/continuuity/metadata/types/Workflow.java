package com.continuuity.metadata.types;

import com.google.common.base.Objects;

/**
 * Represents a workflow.
 */
public class Workflow {
  private final String id;
  private final String application;
  private String name;

  public Workflow(String id, String application) {
    this.id = id;
    this.application = application;
  }

  public String getId() {
    return id;
  }

  public String getApplication() {
    return application;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("application", application)
                  .add("name", name)
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
    Workflow other = (Workflow) o;
    return Objects.equal(id, other.id)
      && Objects.equal(application, other.application)
      && Objects.equal(name, other.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, application, name);
  }

}
