/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.internal.dataset.DatasetCreationSpec;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents an application returned for /apps/{app-id}.
 */
public class ApplicationDetail {

  private final String name;
  private final String appVersion;
  private final String description;
  @Nullable
  private final ChangeDetail change;
  @Nullable
  private final SourceControlMeta sourceControlMeta;
  private final String configuration;
  private final List<DatasetDetail> datasets;
  private final List<ProgramRecord> programs;
  private final List<PluginDetail> plugins;
  private final ArtifactSummary artifact;
  @Nullable
  private final PreferencesDetail preferences;
  @Nullable
  private final List<ScheduleDetail> schedules;
  @SerializedName("principal")
  private final String ownerPrincipal;

  public ApplicationDetail(String name,
      String appVersion,
      String description,
      @Nullable ChangeDetail change,
      @Nullable SourceControlMeta sourceControlMeta,
      String configuration,
      List<DatasetDetail> datasets,
      List<ProgramRecord> programs,
      List<PluginDetail> plugins,
      ArtifactSummary artifact,
      @Nullable PreferencesDetail preferences,
      @Nullable List<ScheduleDetail> schedules,
      @Nullable String ownerPrincipal) {
    this.name = name;
    this.appVersion = appVersion;
    this.description = description;
    this.change = change;
    this.sourceControlMeta = sourceControlMeta;
    this.configuration = configuration;
    this.datasets = datasets;
    this.programs = programs;
    this.plugins = plugins;
    this.artifact = artifact;
    this.schedules = schedules;
    this.preferences = preferences;
    this.ownerPrincipal = ownerPrincipal;
  }


  /**
   * Constructor for backwards compatibility, please do not remove.
   */
  public ApplicationDetail(String name,
      String appVersion,
      String description,
      String configuration,
      List<DatasetDetail> datasets,
      List<ProgramRecord> programs,
      List<PluginDetail> plugins,
      ArtifactSummary artifact,
      @Nullable String ownerPrincipal) {
    this(name, appVersion, description, null, null,
        configuration, datasets, programs, plugins, artifact, null, null, ownerPrincipal);
  }

  /**
   * Constructor for backwards compatibility, please do not remove.
   */
  public ApplicationDetail(String name,
      String appVersion,
      String description,
      @Nullable ChangeDetail change,
      String configuration,
      List<DatasetDetail> datasets,
      List<ProgramRecord> programs,
      List<PluginDetail> plugins,
      ArtifactSummary artifact,
      @Nullable String ownerPrincipal) {
    this(name, appVersion, description, change, null,
        configuration, datasets, programs, plugins, artifact, null, null, ownerPrincipal);
  }

  public String getName() {
    return name;
  }

  public String getAppVersion() {
    return appVersion;
  }

  public String getDescription() {
    return description;
  }

  public String getConfiguration() {
    return configuration;
  }

  @Nullable
  public ChangeDetail getChange() {
    return change;
  }

  @Nullable
  public SourceControlMeta getSourceControlMeta() {
    return sourceControlMeta;
  }

  public List<DatasetDetail> getDatasets() {
    return datasets;
  }

  public List<PluginDetail> getPlugins() {
    return plugins;
  }

  public List<ProgramRecord> getPrograms() {
    return programs;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  public PreferencesDetail getPreferences() {
    return preferences;
  }

  public List<ScheduleDetail> getSchedules() {
    return schedules;
  }

  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }

  public static ApplicationDetail fromSpec(ApplicationSpecification spec,
      @Nullable String ownerPrincipal,
      @Nullable ChangeDetail change,
      @Nullable SourceControlMeta sourceControlMeta,
      @Nullable PreferencesDetail preferences,
      @Nullable List<ScheduleDetail> schedules
  ) {
    // Adding owner, creation time and change summary description fields to the app detail

    List<ProgramRecord> programs = new ArrayList<>();
    for (ProgramSpecification programSpec : spec.getMapReduce().values()) {
      programs.add(new ProgramRecord(ProgramType.MAPREDUCE, spec.getName(),
          programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getServices().values()) {
      programs.add(new ProgramRecord(ProgramType.SERVICE, spec.getName(),
          programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getSpark().values()) {
      programs.add(new ProgramRecord(ProgramType.SPARK, spec.getName(),
          programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getWorkers().values()) {
      programs.add(new ProgramRecord(ProgramType.WORKER, spec.getName(),
          programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getWorkflows().values()) {
      programs.add(new ProgramRecord(ProgramType.WORKFLOW, spec.getName(),
          programSpec.getName(), programSpec.getDescription()));
    }

    List<DatasetDetail> datasets = new ArrayList<>();
    for (DatasetCreationSpec datasetSpec : spec.getDatasets().values()) {
      datasets.add(new DatasetDetail(datasetSpec.getInstanceName(), datasetSpec.getTypeName()));
    }

    List<PluginDetail> plugins = new ArrayList<>();
    for (Map.Entry<String, Plugin> pluginEntry : spec.getPlugins().entrySet()) {
      plugins.add(new PluginDetail(pluginEntry.getKey(),
          pluginEntry.getValue().getPluginClass().getName(),
          pluginEntry.getValue().getPluginClass().getType()));
    }
    // this is only required if there are old apps lying around that failed to get upgrading during
    // the upgrade to v3.2 for some reason. In those cases artifact id will be null until they re-deploy the app.
    // in the meantime, we don't want this api call to null pointer exception.
    ArtifactSummary summary = spec.getArtifactId() == null
        ? new ArtifactSummary(spec.getName(), null) : ArtifactSummary.from(spec.getArtifactId());
    return new ApplicationDetail(spec.getName(), spec.getAppVersion(), spec.getDescription(),
        change, sourceControlMeta,
        spec.getConfiguration(), datasets, programs, plugins, summary, preferences, schedules,
        ownerPrincipal);
  }
}
