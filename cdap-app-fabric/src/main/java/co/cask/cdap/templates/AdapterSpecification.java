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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;

import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Specification of an adapter.
 */
public final class AdapterSpecification {

  private static final EnumSet<ProgramType> ADAPTER_PROGRAM_TYPES = EnumSet.of(ProgramType.WORKFLOW,
                                                                               ProgramType.WORKER);
  private final String name;
  private final String description;
  private final Id.Program program;
  private final ScheduleSpecification scheduleSpec;
  private final int instances;
  private final Map<String, StreamSpecification> streams;
  private final Map<String, DatasetCreationSpec> datasets;
  private final Map<String, String> datasetModules;
  private final Map<String, String> runtimeArgs;
  private final Resources resources;
  // this is a json representation of some config that templates will use to configure
  // an adapter. At configuration time it will be translated into the correct object,
  // but the platform itself never interprets it but just passes it along.
  private final JsonElement config;

  private AdapterSpecification(String name, String description, Id.Program program,
                               ScheduleSpecification scheduleSpec, int instances,
                               Map<String, StreamSpecification> streams,
                               Map<String, DatasetCreationSpec> datasets,
                               Map<String, String> datasetModules,
                               Map<String, String> runtimeArgs, Resources resources,
                               JsonElement config) {
    this.name = name;
    this.description = description;
    this.program = program;
    this.scheduleSpec = scheduleSpec;
    this.instances = instances;
    this.streams = streams == null ? ImmutableMap.<String, StreamSpecification>of() : ImmutableMap.copyOf(streams);
    this.datasets = datasets == null ? ImmutableMap.<String, DatasetCreationSpec>of() : ImmutableMap.copyOf(datasets);
    this.datasetModules = datasetModules == null ?
      ImmutableMap.<String, String>of() : ImmutableMap.copyOf(datasetModules);
    this.runtimeArgs = runtimeArgs == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(runtimeArgs);
    this.config = config;
    this.resources = resources;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getTemplate() {
    return program.getApplicationId();
  }

  public Id.Program getProgram() {
    return program;
  }

  public Map<String, String> getRuntimeArgs() {
    return runtimeArgs;
  }

  @Nullable
  public ScheduleSpecification getScheduleSpec() {
    return scheduleSpec;
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

  @Nullable
  public Integer getInstances() {
    return instances;
  }

  public Resources getResources() {
    return resources;
  }

  public JsonElement getConfig() {
    return config;
  }

  public static Builder builder(String name, Id.Program program) {
    return new Builder(name, program);
  }

  /**
   * Builder for AdapterSpecifications.
   */
  public static class Builder {
    private final String name;
    private final Id.Program program;
    private String description;
    private ScheduleSpecification schedule;
    private Map<String, String> runtimeArgs;
    private Map<String, StreamSpecification> streams;
    private Map<String, DatasetCreationSpec> datasets;
    private Map<String, String> datasetModules;
    private int instances;
    private Resources resources;
    private JsonElement config;

    public Builder(String name, Id.Program program) {
      Preconditions.checkArgument(name != null, "Adapter name must be specified.");
      Preconditions.checkArgument(ADAPTER_PROGRAM_TYPES.contains(program.getType()), "Adapter program type must be " +
        "one of these: %s" , ADAPTER_PROGRAM_TYPES);
      this.name = name;
      this.program = program;
      // defaults
      this.instances = 1;
      this.description = "";
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder setScheduleSpec(ScheduleSpecification schedule) {
      this.schedule = schedule;
      return this;
    }

    public Builder setRuntimeArgs(Map<String, String> runtimeArgs) {
      this.runtimeArgs = runtimeArgs;
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

    public Builder setResources(Resources resources) {
      this.resources = resources;
      return this;
    }

    public AdapterSpecification build() {
      return new AdapterSpecification(name, description, program, schedule, instances,
                                      streams, datasets, datasetModules, runtimeArgs, resources, config);
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
      Objects.equal(program, that.program) &&
      Objects.equal(config, that.config) &&
      Objects.equal(scheduleSpec, that.scheduleSpec) &&
      Objects.equal(runtimeArgs, that.runtimeArgs) &&
      Objects.equal(streams, that.streams) &&
      Objects.equal(datasets, that.datasets) &&
      Objects.equal(datasetModules, that.datasetModules) &&
      Objects.equal(instances, that.instances);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, program, config, scheduleSpec, runtimeArgs,
                            streams, datasets, datasetModules, instances);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("description", description)
      .add("program", program)
      .add("config", config)
      .add("scheduleSpec", scheduleSpec)
      .add("runtimeargs", runtimeArgs)
      .add("streams", streams)
      .add("datasets", datasets)
      .add("datasetModules", datasetModules)
      .add("instances", instances)
      .toString();
  }
}
