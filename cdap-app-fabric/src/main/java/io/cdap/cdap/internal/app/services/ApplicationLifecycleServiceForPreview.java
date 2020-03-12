/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.data2.datafabric.dataset.DatasetServiceFetcher;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * Service that manage lifecycle of Applications.
 */
public class ApplicationLifecycleServiceForPreview extends AbstractIdleService { //
  public static final String LOCATION_FACTORY = "ApplicationLifecycleServiceForPreviewLocationFactory";

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationLifecycleServiceForPreview.class);

  /**
   * Store manages non-runtime lifecycle.
   */
  private final OwnerAdmin ownerAdmin;
  private final ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final boolean appUpdateSchedules;
  private final AdminEventPublisher adminEventPublisher;
  private final ArtifactRepository artifactRepository;
  private final DatasetServiceFetcher datasetServiceFetcher;
  private final LocationFactory locationFactory;

  @Inject
  ApplicationLifecycleServiceForPreview(CConfiguration cConfiguration,
                                        OwnerAdmin ownerAdmin,
                                        ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
                                        AuthorizationEnforcer authorizationEnforcer,
                                        AuthenticationContext authenticationContext,
                                        MessagingService messagingService,
                                        ArtifactRepository ArtifactRepository,
                                        DatasetServiceFetcher datasetServiceFetcher,
                                        @Named(LOCATION_FACTORY) LocationFactory locationFactory) {
    this.appUpdateSchedules = cConfiguration.getBoolean(Constants.AppFabric.APP_UPDATE_SCHEDULES,
                                                        Constants.AppFabric.DEFAULT_APP_UPDATE_SCHEDULES);
    this.managerFactory = managerFactory;
    this.ownerAdmin = ownerAdmin;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.adminEventPublisher = new AdminEventPublisher(cConfiguration,
                                                       new MultiThreadMessagingContext(messagingService));
    this.artifactRepository = ArtifactRepository;
    this.datasetServiceFetcher = datasetServiceFetcher;
    this.locationFactory = locationFactory;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ApplicationLifecycleServiceForPreview");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down ApplicationLifecycleServiceForPreview");
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed, the Application
   * class is instantiated and configure() is called in order to generate an {@link ApplicationSpecification}.
   * Programs, datasets, and streams are created based on the specification before the spec is persisted in the
   * {@link Store}. This method can create a new application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param summary the artifact summary of the app
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   * For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @param ownerPrincipal the kerberos principal of the application owner
   * @param updateSchedules specifies if schedules of the workflow have to be updated,
   * if null value specified by the property "app.deploy.update.schedules" will be used.
   * @return information about the deployed application
   * @throws InvalidArtifactException  if the artifact does not contain any application classes
   * @throws IOException               if there was an IO error reading artifact detail from the meta store
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws Exception                 if there was an exception during the deployment pipeline. This exception will often wrap
   *                                   the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace, @Nullable String appName, @Nullable String appVersion,
                                           ArtifactSummary summary,
                                           @Nullable String configStr,
                                           ProgramTerminator programTerminator,
                                           @Nullable KerberosPrincipalId ownerPrincipal,
                                           @Nullable Boolean updateSchedules) throws Exception {
    doSomethingelse();
    LOG.debug("wyzhang: ApplicationLifecycleServiceForPreview::deployApp() start");
    NamespaceId artifactNamespace =
      ArtifactScope.SYSTEM.equals(summary.getScope()) ? NamespaceId.SYSTEM : namespace;
    Id.Artifact artifactId = new Id.Artifact(artifactNamespace.getNamespace(), summary.getName(), summary.getVersion());
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    LOG.debug("wyzhang: ApplicationLifecycleServiceForPreview::deployApp() artifact detail fetched: " + artifactDetail.getDescriptor().getLocationURI());
    Location artifactLocation =
      Locations.getLocationFromAbsolutePath(locationFactory,
                                            artifactDetail.getDescriptor().getLocationURI().getPath());
    LOG.debug("wyzhang: ApplicationLifecycleServiceForPreview::deployApp() locationFactory = " + locationFactory.getClass().getName());
    LOG.debug("wyzhang: ApplicationLifecycleServiceForPreview::deployApp() location obj " + artifactLocation.toURI());
    ArtifactDetail artifactDetailFull =
      new ArtifactDetail(new ArtifactDescriptor(artifactDetail.getDescriptor().getArtifactId(), artifactLocation),
                         artifactDetail.getMeta());
    LOG.debug("wyzhang: ApplicationLifecycleServiceForPreview::deployApp() artifact detial = " + artifactDetail.toString());
    return deployApp(namespace, appName, appVersion, configStr, programTerminator, artifactDetailFull,
                     ownerPrincipal, updateSchedules == null ? appUpdateSchedules : updateSchedules);
  }

  private ApplicationWithPrograms deployApp(NamespaceId namespaceId, @Nullable String appName,
                                            @Nullable String appVersion,
                                            @Nullable String configStr,
                                            ProgramTerminator programTerminator,
                                            ArtifactDetail artifactDetail,
                                            @Nullable KerberosPrincipalId ownerPrincipal,
                                            boolean updateSchedules) throws Exception {
    // Now to deploy an app, we need ADMIN privilege on the owner principal if it is present, and also ADMIN on the app
    // But since at this point, app name is unknown to us, so the enforcement on the app is happening in the deploy
    // pipeline - LocalArtifactLoaderStage

    // need to enforce on the principal id if impersonation is involved
    KerberosPrincipalId effectiveOwner =
      SecurityUtil.getEffectiveOwner(ownerAdmin, namespaceId,
                                     ownerPrincipal == null ? null : ownerPrincipal.getPrincipal());


    Principal requestingUser = authenticationContext.getPrincipal();
    // enforce that the current principal, if not the same as the owner principal, has the admin privilege on the
    // impersonated principal
    if (effectiveOwner != null) {
      authorizationEnforcer.enforce(effectiveOwner, requestingUser, Action.ADMIN);
    }

    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null);
    if (appClass == null) {
      throw new InvalidArtifactException(String.format("No application class found in artifact '%s' in namespace '%s'.",
                                                       artifactDetail.getDescriptor().getArtifactId(), namespaceId));
    }

    // deploy application with newly added artifact
    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactDetail.getDescriptor(), namespaceId,
                                                             appClass.getClassName(), appName, appVersion,
                                                             configStr, ownerPrincipal, updateSchedules);
    LOG.debug("wyzhang: ApplicationLifecycleServiceForPreview::deployApp() deployment info = " + deploymentInfo.toString());

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(programTerminator);
    // TODO: (CDAP-3258) Manager needs MUCH better error handling.
    ApplicationWithPrograms applicationWithPrograms;
    try {
      applicationWithPrograms = manager.deploy(deploymentInfo).get();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), Exception.class);
      throw Throwables.propagate(e.getCause());
    }

    adminEventPublisher.publishAppCreation(applicationWithPrograms.getApplicationId(),
                                           applicationWithPrograms.getSpecification());
    return applicationWithPrograms;
  }

  private void doSomethingelse() {
    LOG.debug("wyzhang: doSomethingelse start");
    try {
      Collection<DatasetSpecificationSummary> result = datasetServiceFetcher.getDatasets("default");
    } catch (Exception e) {
      LOG.debug("wyzhang: doSomethingelse get exception " + e);
    }
    LOG.debug("wyzhang: doSomethingelse end");
  }
}
