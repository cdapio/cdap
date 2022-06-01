/*
 * Copyright © 2022 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 * App upgrade service that upgrade applications every 5 minutes
 */
public class AppUpgradeService extends AbstractRetryableScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(AppUpgradeService.class);
  private final long scheduleIntervalInMillis;
  private final NamespaceAdmin namespaceAdmin;
  private final ArtifactRepository artifactRepository;
  private final Store store;
  private final ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory;

  @Inject
  protected AppUpgradeService(NamespaceAdmin namespaceAdmin, ArtifactRepository artifactRepository, Store store,
                              ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory) {
    super(RetryStrategies.fixDelay(5, TimeUnit.MINUTES));
    this.scheduleIntervalInMillis = TimeUnit.MINUTES.toMillis(5);
    this.namespaceAdmin = namespaceAdmin;
    this.artifactRepository = artifactRepository;
    this.store = store;
    this.managerFactory = managerFactory;
  }

  @Override
  protected long runTask() throws Exception {
    List<NamespaceMeta> namespaces = namespaceAdmin.list();
    for (NamespaceMeta namespace : namespaces) {
      Collection<ApplicationSpecification> applications = store.getAllApplications(namespace.getNamespaceId());
      for (ApplicationSpecification app : applications) {
        try {
          upgradeApp(app, namespace.getNamespaceId());
        } catch (Exception ex) {
          LOG.warn("Failed to upgrade app {}", app.getName());
        }
      }
    }
    return scheduleIntervalInMillis;
  }

  private void upgradeApp(ApplicationSpecification appSpec, NamespaceId namespaceId) throws Exception {
    ArtifactId oldArtifactId = Artifacts.toProtoArtifactId(namespaceId, appSpec.getArtifactId());
    ArtifactDetail artifactDetail = getLatestArtifact(oldArtifactId);
    ArtifactId artifactId = Artifacts.toProtoArtifactId(namespaceId, artifactDetail.getDescriptor().getArtifactId());
    if (artifactId.equals(oldArtifactId)) {
      return;
    }
    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null);

    String oldVersion = appSpec.getAppVersion();

    String appVersion = oldVersion.equals(ApplicationId.DEFAULT_VERSION) ? "v1" :
                          "v" + (Integer.parseInt(oldVersion.substring(1)) + 1);
    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactId, artifactDetail.getDescriptor().getLocation(),
                                                             namespaceId, appClass, appSpec.getName(),
                                                             appVersion, appSpec.getConfiguration(), null,
                                                             true, null);

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(programId -> { });
    ListenableFuture<ApplicationWithPrograms> future = manager.deploy(deploymentInfo);
    future.get();
  }

  protected ArtifactDetail getLatestArtifact(ArtifactId artifactId) throws Exception {
    ArtifactVersionRange versionRange = new ArtifactVersionRange(new ArtifactVersion("0.0.0"), true,
                                                                 new ArtifactVersion("10.0.0"), true);
    List<ArtifactDetail> artifactDetails = artifactRepository.getArtifactDetails(
      new ArtifactRange(artifactId.getNamespace(), artifactId.getArtifact(), versionRange), 1, ArtifactSortOrder.DESC);
    return artifactDetails.get(0);
  }
}
