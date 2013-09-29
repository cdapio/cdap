package com.continuuity.metadata.types;

import com.google.common.base.Objects;

/**
 * Represents a dataset.
 */
public class Dataset {

  private final String id;
  private String name;
  private String description;
  private String type;
  private String specification;

  public Dataset(String id) {
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

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSpecification() {
    return specification;
  }

  public void setSpecification(String specification) {
    this.specification = specification;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dataset dataset = (Dataset) o;
    return Objects.equal(id, dataset.id) && Objects.equal(name, dataset.name) && Objects.equal(type, dataset.type)
      && Objects.equal(description, dataset.description) && Objects.equal(specification, dataset.specification);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, type, description, specification);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("name", name)
                  .add("description", description)
                  .add("type", type)
                  .add("specification", specification)
                  .toString();
  }
}
