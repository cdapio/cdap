/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.cdap;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.operations.OperationalStats;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Predicates;
import com.google.inject.Injector;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * {@link OperationalStats} for reporting CDAP entities.
 */
public class CDAPEntities extends AbstractCDAPStats implements CDAPEntitiesMXBean {
  private NamespaceQueryAdmin nsQueryAdmin;
  private ApplicationLifecycleService appLifecycleService;
  private ProgramLifecycleService programLifecycleService;
  private ArtifactRepository artifactRepository;
  private DatasetFramework dsFramework;
  private StreamAdmin streamAdmin;
  private int namespaces;
  private int artifacts;
  private int apps;
  private int programs;
  private int datasets;
  private int streams;
  private int streamViews;

  @Override
  public void initialize(Injector injector) {
    nsQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    appLifecycleService = injector.getInstance(ApplicationLifecycleService.class);
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    artifactRepository = injector.getInstance(ArtifactRepository.class);
    dsFramework = injector.getInstance(DatasetFramework.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
  }

  @Override
  public String getStatType() {
    return "entities";
  }

  @Override
  public int getNamespaces() {
    return namespaces;
  }

  @Override
  public int getArtifacts() {
    return artifacts;
  }

  @Override
  public int getApplications() {
    return apps;
  }

  @Override
  public int getPrograms() {
    return programs;
  }

  @Override
  public int getDatasets() {
    return datasets;
  }

  @Override
  public int getStreams() {
    return streams;
  }

  @Override
  public int getStreamViews() {
    return streamViews;
  }

  @Override
  public void collect() throws Exception {
    reset();
    List<NamespaceMeta> namespaceMetas;
    namespaceMetas = nsQueryAdmin.list();
    namespaces = namespaceMetas.size();
    for (NamespaceMeta meta : namespaceMetas) {
      List<ApplicationRecord> appRecords =
        appLifecycleService.getApps(meta.getNamespaceId(), Predicates.<ApplicationRecord>alwaysTrue());
      apps += appRecords.size();
      Set<ProgramType> programTypes = EnumSet.of(ProgramType.FLOW, ProgramType.MAPREDUCE, ProgramType.SERVICE,
                                                 ProgramType.SPARK, ProgramType.WORKER, ProgramType.WORKFLOW);
      for (ProgramType programType : programTypes) {
        programs += programLifecycleService.list(meta.getNamespaceId(), programType).size();
      }
      artifacts += artifactRepository.getArtifactSummaries(meta.getNamespaceId(), true).size();
      datasets += dsFramework.getInstances(meta.getNamespaceId()).size();
      List<StreamSpecification> streamSpecs = streamAdmin.listStreams(meta.getNamespaceId());
      streams += streamSpecs.size();
      for (StreamSpecification streamSpec : streamSpecs) {
        StreamId streamId = meta.getNamespaceId().stream(streamSpec.getName());
        streamViews += streamAdmin.listViews(streamId).size();
      }
    }
  }

  private void reset() {
    namespaces = 0;
    artifacts = 0;
    apps = 0;
    programs = 0;
    datasets = 0;
    streams = 0;
    streamViews = 0;
  }
}
