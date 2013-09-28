package com.continuuity.metadata.types;

import com.google.common.base.Objects;

import java.util.List;

/**
 * Represents a map/reduce program.
 */
public class Mapreduce {

  private final String id;
  private final String application;
  private String name;
  private String description;
  private List<String> datasets;

  public Mapreduce(String id, String application) {
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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<String> getDatasets() {
    return datasets;
  }

  public void setDatasets(List<String> datasets) {
    this.datasets = datasets;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("application", application)
                  .add("name", name)
                  .add("description", description)
                  .add("datasets", datasets)
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
    Mapreduce other = (Mapreduce) o;
    return Objects.equal(id, other.id)
      && Objects.equal(application, other.application)
      && Objects.equal(name, other.name)
      && Objects.equal(description, other.description)
      && Objects.equal(datasets, other.datasets);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, application, name, description, datasets);
  }
}
