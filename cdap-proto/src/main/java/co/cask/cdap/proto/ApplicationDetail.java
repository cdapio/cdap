/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.artifact.ArtifactSummary;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an application returned for /apps/{app-id}.
 */
public class ApplicationDetail {
  private final String name;
  private final String version;
  private final String description;
  private final String configuration;
  private final List<StreamDetail> streams;
  private final List<DatasetDetail> datasets;
  private final List<ProgramRecord> programs;
  private final ArtifactSummary artifact;

  public ApplicationDetail(String name,
                           String description,
                           String configuration,
                           List<StreamDetail> streams,
                           List<DatasetDetail> datasets,
                           List<ProgramRecord> programs,
                           ArtifactSummary artifact) {
    this.name = name;
    this.version = artifact.getVersion();
    this.description = description;
    this.configuration = configuration;
    this.streams = streams;
    this.datasets = datasets;
    this.programs = programs;
    this.artifact = artifact;
  }

  public String getName() {
    return name;
  }

  /**
   * @deprecated use {@link #getArtifact()} instead
   *
   * @return the version of the artifact used to create the application
   */
  @Deprecated
  public String getVersion() {
    return version;
  }

  public String getDescription() {
    return description;
  }

  public String getConfiguration() {
    return configuration;
  }

  public List<StreamDetail> getStreams() {
    return streams;
  }

  public List<DatasetDetail> getDatasets() {
    return datasets;
  }

  public List<ProgramRecord> getPrograms() {
    return programs;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  public static ApplicationDetail fromSpec(ApplicationSpecification spec) {
    List<ProgramRecord> programs = new ArrayList<>();
    for (ProgramSpecification programSpec : spec.getFlows().values()) {
      programs.add(new ProgramRecord(ProgramType.FLOW, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }
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

    List<StreamDetail> streams = new ArrayList<>();
    for (StreamSpecification streamSpec : spec.getStreams().values()) {
      streams.add(new StreamDetail(streamSpec.getName()));
    }

    List<DatasetDetail> datasets = new ArrayList<>();
    for (DatasetCreationSpec datasetSpec : spec.getDatasets().values()) {
      datasets.add(new DatasetDetail(datasetSpec.getInstanceName(), datasetSpec.getTypeName()));
    }

    // this is only required if there are old apps lying around that failed to get upgrading during
    // the upgrade to v3.2 for some reason. In those cases artifact id will be null until they re-deploy the app.
    // in the meantime, we don't want this api call to null pointer exception.
    ArtifactSummary summary = spec.getArtifactId() == null ?
      new ArtifactSummary(spec.getName(), null) : ArtifactSummary.from(spec.getArtifactId());
    return new ApplicationDetail(spec.getName(), spec.getDescription(), spec.getConfiguration(),
                                 streams, datasets, programs, summary);
  }
}
