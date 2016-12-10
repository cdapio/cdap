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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.security.ImpersonationUtils;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.view.ViewAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.AppSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.ArtifactSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.DatasetSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.ProgramSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.StreamSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.SystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.ViewSystemMetadataWriter;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.store.NamespaceStore;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Updates system metadata for existing entities.
 */
class ExistingEntitySystemMetadataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ExistingEntitySystemMetadataWriter.class);

  private final MetadataStore metadataStore;
  private final NamespaceStore nsStore;
  private final Store store;
  private final StreamAdmin streamAdmin;
  private final ViewAdmin viewAdmin;
  private final ArtifactStore artifactStore;
  private final LocationFactory locationFactory;
  private final CConfiguration cConf;
  private final Impersonator impersonator;

  @Inject
  ExistingEntitySystemMetadataWriter(MetadataStore metadataStore, NamespaceStore nsStore, Store store,
                                     ArtifactStore artifactStore, StreamAdmin streamAdmin, ViewAdmin viewAdmin,
                                     LocationFactory locationFactory, CConfiguration cConf, Impersonator impersonator) {
    this.metadataStore = metadataStore;
    this.nsStore = nsStore;
    this.store = store;
    this.streamAdmin = streamAdmin;
    this.viewAdmin = viewAdmin;
    this.artifactStore = artifactStore;
    this.locationFactory = locationFactory;
    this.cConf = cConf;
    this.impersonator = impersonator;
  }

  void write(DatasetFramework dsFramework) throws Exception {
    for (NamespaceMeta namespaceMeta : nsStore.list()) {
      NamespaceId namespace = new NamespaceId(namespaceMeta.getName());
      writeSystemMetadataForArtifacts(namespace);
      writeSystemMetadataForApps(namespace);
      writeSystemMetadataForDatasets(namespace, dsFramework);
      writeSystemMetadataForStreams(namespace);
    }
  }

  private void writeSystemMetadataForArtifacts(NamespaceId namespace) throws IOException {
    for (ArtifactDetail artifactDetail : artifactStore.getArtifacts(namespace)) {
      co.cask.cdap.api.artifact.ArtifactId artifact = artifactDetail.getDescriptor().getArtifactId();
      ArtifactInfo artifactInfo = new ArtifactInfo(artifact,
                                                   artifactDetail.getMeta().getClasses(),
                                                   artifactDetail.getMeta().getProperties());
      ArtifactId artifactId = namespace.artifact(artifact.getName(), artifact.getVersion().getVersion());
      SystemMetadataWriter writer = new ArtifactSystemMetadataWriter(metadataStore, artifactId, artifactInfo);
      writer.write();
    }
  }

  private void writeSystemMetadataForApps(NamespaceId namespace) {
    for (ApplicationSpecification appSpec : store.getAllApplications(namespace)) {
      ApplicationId app = namespace.app(appSpec.getName());
      SystemMetadataWriter writer = new AppSystemMetadataWriter(metadataStore, app, appSpec);
      writer.write();
      writeSystemMetadataForPrograms(app, appSpec);
    }
  }

  private void writeSystemMetadataForPrograms(ApplicationId app, ApplicationSpecification appSpec) {
    writeSystemMetadataForPrograms(app, ProgramType.FLOW, appSpec.getFlows().values());
    writeSystemMetadataForPrograms(app, ProgramType.MAPREDUCE, appSpec.getMapReduce().values());
    writeSystemMetadataForPrograms(app, ProgramType.SERVICE, appSpec.getServices().values());
    writeSystemMetadataForPrograms(app, ProgramType.SPARK, appSpec.getSpark().values());
    writeSystemMetadataForPrograms(app, ProgramType.WORKER, appSpec.getWorkers().values());
    writeSystemMetadataForPrograms(app, ProgramType.WORKFLOW, appSpec.getWorkflows().values());
  }

  private void writeSystemMetadataForPrograms(ApplicationId app, ProgramType programType,
                                              Collection<? extends ProgramSpecification> programSpecs) {
    for (ProgramSpecification programSpec : programSpecs) {
      ProgramId programId = app.program(programType, programSpec.getName());
      SystemMetadataWriter writer = new ProgramSystemMetadataWriter(metadataStore, programId, programSpec, true);
      writer.write();
    }
  }

  private void writeSystemMetadataForDatasets(NamespaceId namespace, DatasetFramework dsFramework)
    throws DatasetManagementException, IOException {
    SystemDatasetInstantiatorFactory systemDatasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(locationFactory, dsFramework, cConf);
    try (SystemDatasetInstantiator systemDatasetInstantiator = systemDatasetInstantiatorFactory.create()) {
      UserGroupInformation ugi = impersonator.getUGI(namespace);

      for (DatasetSpecificationSummary summary : dsFramework.getInstances(namespace)) {
        final DatasetId dsInstance = namespace.dataset(summary.getName());
        DatasetProperties dsProperties = DatasetProperties.of(summary.getProperties());
        String dsType = summary.getType();
        Dataset dataset = null;
        try {
          try {
            dataset = ImpersonationUtils.doAs(ugi, new Callable<Dataset>() {
              @Override
              public Dataset call() throws Exception {
                return systemDatasetInstantiator.getDataset(dsInstance);
              }
            });
          } catch (Exception e) {
            LOG.warn("Exception while instantiating dataset {}", dsInstance, e);
          }

          SystemMetadataWriter writer =
            new DatasetSystemMetadataWriter(metadataStore, dsInstance, dsProperties, dataset, dsType,
                                            summary.getDescription());
          writer.write();
        } finally {
          if (dataset != null) {
            dataset.close();
          }
        }
      }
    }
  }

  private void writeSystemMetadataForStreams(NamespaceId namespace) throws Exception {
    for (StreamSpecification streamSpec : store.getAllStreams(namespace)) {
      StreamId streamId = namespace.stream(streamSpec.getName());
      SystemMetadataWriter writer =
        new StreamSystemMetadataWriter(metadataStore, streamId, streamAdmin.getConfig(streamId),
                                       streamSpec.getDescription());
      writer.write();
      for (StreamViewId view : streamAdmin.listViews(streamId)) {
        writer = new ViewSystemMetadataWriter(metadataStore, view, viewAdmin.get(view), true);
        writer.write();
      }
    }
  }
}
