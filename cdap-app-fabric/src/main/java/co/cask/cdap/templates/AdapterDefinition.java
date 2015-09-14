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
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import java.lang.reflect.Type;
import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Specification of an adapter. Only here because needed for CDAP upgrade.
 */
@Deprecated
public final class AdapterDefinition {

  private static final Gson GSON = new Gson();
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
  private final Map<String, AdapterPlugin> plugins;
  private final Resources resources;
  // this is a json representation of some config that templates will use to configure
  // an adapter. At configuration time it will be translated into the correct object,
  // but the platform itself never interprets it but just passes it along.
  private final JsonElement config;

  private AdapterDefinition(String name, String description, Id.Program program,
                            ScheduleSpecification scheduleSpec, int instances,
                            Map<String, StreamSpecification> streams,
                            Map<String, DatasetCreationSpec> datasets,
                            Map<String, String> datasetModules,
                            Map<String, String> runtimeArgs,
                            Map<String, AdapterPlugin> plugins,
                            Resources resources,
                            JsonElement config) {
    this.name = name;
    this.description = description;
    this.program = program;
    this.scheduleSpec = scheduleSpec;
    this.instances = instances;
    this.streams = streams;
    this.datasets = datasets;
    this.datasetModules = datasetModules;
    this.runtimeArgs = runtimeArgs;
    this.plugins = plugins;
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

  public ScheduleSpecification getScheduleSpecification() {
    return scheduleSpec;
  }

  public Map<String, StreamSpecification> getStreams() {
    return streams;
  }

  public Map<String, DatasetCreationSpec> getDatasets() {
    return datasets;
  }

  public Map<String, AdapterPlugin> getPlugins() {
    return plugins;
  }

  @Nullable
  public Integer getInstances() {
    return instances;
  }

  public Resources getResources() {
    return resources;
  }

  @Nullable
  public JsonElement getConfig() {
    return config;
  }

  public <T> T getConfig(Type configType) {
    return config == null ? null : GSON.<T>fromJson(config, configType);
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
    private Map<String, String> runtimeArgs = ImmutableMap.of();
    private Map<String, StreamSpecification> streams = ImmutableMap.of();
    private Map<String, DatasetCreationSpec> datasets = ImmutableMap.of();
    private Map<String, String> datasetModules = ImmutableMap.of();
    private Map<String, AdapterPlugin> plugins = ImmutableMap.of();
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
      this.runtimeArgs = ImmutableMap.copyOf(runtimeArgs);
      return this;
    }

    public Builder setStreams(Map<String, StreamSpecification> streams) {
      this.streams = ImmutableMap.copyOf(streams);
      return this;
    }

    public Builder setDatasets(Map<String, DatasetCreationSpec> datasets) {
      this.datasets = ImmutableMap.copyOf(datasets);
      return this;
    }

    public Builder setPlugins(Map<String, AdapterPlugin> plugins) {
      this.plugins = ImmutableMap.copyOf(plugins);
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

    public AdapterDefinition build() {
      return new AdapterDefinition(name, description, program, schedule, instances,
                                      streams, datasets, datasetModules, runtimeArgs, plugins, resources, config);
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

    AdapterDefinition that = (AdapterDefinition) o;

    return Objects.equal(name, that.name) &&
      Objects.equal(description, that.description) &&
      Objects.equal(program, that.program) &&
      Objects.equal(config, that.config) &&
      Objects.equal(scheduleSpec, that.scheduleSpec) &&
      Objects.equal(runtimeArgs, that.runtimeArgs) &&
      Objects.equal(streams, that.streams) &&
      Objects.equal(datasets, that.datasets) &&
      Objects.equal(datasetModules, that.datasetModules) &&
      Objects.equal(instances, that.instances) &&
      Objects.equal(plugins, that.plugins);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, program, config, scheduleSpec, runtimeArgs,
                            streams, datasets, datasetModules, instances, plugins);
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
      .add("plugins", plugins)
      .toString();
  }

}
