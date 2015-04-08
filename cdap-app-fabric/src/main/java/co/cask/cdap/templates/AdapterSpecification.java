/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.templates;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.sun.istack.Nullable;

import java.util.Map;

/**
 * Specification of an adapter.
 */
public final class AdapterSpecification {
  private final String name;
  private final String description;
  private final String template;
  private final ScheduleSpecification scheduleSpec;
  private final WorkerSpecification workerSpec;
  private final Map<String, StreamSpecification> streams;
  private final Map<String, DatasetCreationSpec> datasets;
  private final Map<String, String> datasetModules;
  private final int instances;
  // this is a json representation of some config that templates will use to configure
  // an adapter. At configuration time it will be translated into the correct object,
  // but the platform itself never interprets it but just passes it along.
  private final JsonElement config;

  private AdapterSpecification(String name, String description, String template,
                               ScheduleSpecification scheduleSpec, WorkerSpecification workerSpec,
                               Map<String, StreamSpecification> streams,
                               Map<String, DatasetCreationSpec> datasets,
                               Map<String, String> datasetModules,
                               int instances, JsonElement config) {
    this.name = name;
    this.description = description;
    this.template = template;
    this.scheduleSpec = scheduleSpec;
    this.workerSpec = workerSpec;
    this.streams = streams == null ? ImmutableMap.<String, StreamSpecification>of() : ImmutableMap.copyOf(streams);
    this.datasets = datasets == null ? ImmutableMap.<String, DatasetCreationSpec>of() : ImmutableMap.copyOf(datasets);
    this.datasetModules = datasetModules == null ?
      ImmutableMap.<String, String>of() : ImmutableMap.copyOf(datasetModules);
    this.instances = instances;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getTemplate() {
    return template;
  }

  @Nullable
  public ScheduleSpecification getScheduleSpec() {
    return scheduleSpec;
  }

  @Nullable
  public WorkerSpecification getWorkerSpec() {
    return workerSpec;
  }

  public Map<String, StreamSpecification> getStreams() {
    return streams;
  }

  public Map<String, DatasetCreationSpec> getDatasets() {
    return datasets;
  }

  public Map<String, String> getDatasetModules() {
    return datasetModules;
  }

  public int getInstances() {
    return instances;
  }

  public JsonElement getConfig() {
    return config;
  }

  public static Builder builder(String name, String template) {
    return new Builder(name, template);
  }

  /**
   * Builder for AdapterSpecifications.
   */
  public static class Builder {
    private final String name;
    private final String template;
    private String description;
    private ScheduleSpecification scheduleSpec;
    private WorkerSpecification workerSpec;
    private Map<String, StreamSpecification> streams;
    private Map<String, DatasetCreationSpec> datasets;
    private Map<String, String> datasetModules;
    private int instances;
    private JsonElement config;

    public Builder(String name, String template) {
      Preconditions.checkArgument(name != null, "Adapter name must be specified.");
      Preconditions.checkArgument(template != null, "Adapter template must be specified.");
      this.name = name;
      this.template = template;
      // defaults
      this.instances = 1;
      this.description = "";
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setScheduleSpec(ScheduleSpecification scheduleSpec) {
      this.scheduleSpec = scheduleSpec;
      return this;
    }

    public Builder setWorkerSpec(WorkerSpecification workerSpec) {
      this.workerSpec = workerSpec;
      return this;
    }

    public Builder setStreams(Map<String, StreamSpecification> streams) {
      this.streams = streams;
      return this;
    }

    public Builder setDatasets(Map<String, DatasetCreationSpec> datasets) {
      this.datasets = datasets;
      return this;
    }

    public Builder setDatasetModules(Map<String, String> modules) {
      this.datasetModules = modules;
      return this;
    }

    public Builder setConfig(JsonElement config) {
      this.config = config;
      return this;
    }

    public Builder setInstances(int instances) {
      this.instances = instances;
      return this;
    }

    public AdapterSpecification build() {
      return new AdapterSpecification(name, description, template, scheduleSpec, workerSpec,
                                      streams, datasets, datasetModules, instances, config);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterSpecification that = (AdapterSpecification) o;

    return Objects.equal(name, that.name) &&
      Objects.equal(description, that.description) &&
      Objects.equal(template, that.template) &&
      Objects.equal(config, that.config) &&
      Objects.equal(scheduleSpec, that.scheduleSpec) &&
      Objects.equal(workerSpec, that.workerSpec) &&
      Objects.equal(streams, that.streams) &&
      Objects.equal(datasets, that.datasets) &&
      Objects.equal(datasetModules, that.datasetModules) &&
      Objects.equal(instances, that.instances);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, template, config, scheduleSpec, workerSpec,
                            streams, datasets, datasetModules, instances);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("description", description)
      .add("template", template)
      .add("config", config)
      .add("scheduleSpec", scheduleSpec)
      .add("workerSpec", workerSpec)
      .add("streams", streams)
      .add("datasets", datasets)
      .add("datasetModules", datasetModules)
      .add("instances", instances)
      .toString();
  }
}
