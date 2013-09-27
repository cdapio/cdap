package com.continuuity.metadata.types;

import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;

/**
 * Represents a flow.
 */
public class Flow {

  private final String id;
  private final String application;
  private String name;
  private List<String> streams = Collections.emptyList();
  private List<String> datasets = Collections.emptyList();

  public Flow(String id, String application) {
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

  public List<String> getStreams() {
    return streams;
  }

  public void setStreams(List<String> streams) {
    this.streams = streams;
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
                  .add("streams", streams)
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
    Flow flow = (Flow) o;
    return Objects.equal(id, flow.id)
      && Objects.equal(application, flow.application)
      && Objects.equal(name, flow.name)
      && Objects.equal(streams, flow.streams)
      && Objects.equal(datasets, flow.datasets);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, application, name, streams, datasets);
  }
}
