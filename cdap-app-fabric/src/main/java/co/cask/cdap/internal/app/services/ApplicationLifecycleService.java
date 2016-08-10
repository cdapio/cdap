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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.CannotBeDeletedException;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.PluginInstanceDetail;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ApplicationClass;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Service that manage lifecycle of Applications.
 */
public class ApplicationLifecycleService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationLifecycleService.class);
  private static final Gson GSON = new Gson();

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;
  private final CConfiguration configuration;
  private final Scheduler scheduler;
  private final QueueAdmin queueAdmin;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final StreamConsumerFactory streamConsumerFactory;
  private final UsageRegistry usageRegistry;
  private final PreferencesStore preferencesStore;
  private final MetricStore metricStore;
  private final ArtifactRepository artifactRepository;
  private final ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final MetadataStore metadataStore;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final Impersonator impersonator;

  @Inject
  ApplicationLifecycleService(ProgramRuntimeService runtimeService, Store store, CConfiguration configuration,
                              Scheduler scheduler, QueueAdmin queueAdmin,
                              NamespacedLocationFactory namespacedLocationFactory,
                              StreamConsumerFactory streamConsumerFactory, UsageRegistry usageRegistry,
                              PreferencesStore preferencesStore, MetricStore metricStore,
                              ArtifactRepository artifactRepository,
                              ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
                              MetadataStore metadataStore,
                              AuthorizerInstantiator authorizerInstantiator,
                              AuthorizationEnforcer authorizationEnforcer,
                              AuthenticationContext authenticationContext, Impersonator impersonator) {
    this.runtimeService = runtimeService;
    this.store = store;
    this.configuration = configuration;
    this.scheduler = scheduler;
    this.queueAdmin = queueAdmin;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.streamConsumerFactory = streamConsumerFactory;
    this.usageRegistry = usageRegistry;
    this.preferencesStore = preferencesStore;
    this.metricStore = metricStore;
    this.artifactRepository = artifactRepository;
    this.managerFactory = managerFactory;
    this.metadataStore = metadataStore;
    this.authorizerInstantiator = authorizerInstantiator;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.impersonator = impersonator;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ApplicationLifecycleService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down ApplicationLifecycleService");
  }

  /**
   * Get all applications in the specified namespace, filtered to only include applications with an artifact name
   * in the set of specified names and an artifact version equal to the specified version. If the specified set
   * is empty, no filtering is performed on artifact name. If the specified version is null, no filtering is done
   * on artifact version.
   *
   * @param namespace the namespace to get apps from
   * @param artifactNames the set of valid artifact names. If empty, all artifact names are valid
   * @param artifactVersion the artifact version to match. If null, all artifact versions are valid
   * @return list of all applications in the namespace that match the specified artifact names and version
   */
  public List<ApplicationRecord> getApps(Id.Namespace namespace,
                                         Set<String> artifactNames,
                                         @Nullable String artifactVersion) throws Exception {
    return getApps(namespace, getAppPredicate(artifactNames, artifactVersion));
  }

  /**
   * Get all applications in the specified namespace that satisfy the specified predicate.
   *
   * @param namespace the namespace to get apps from
   * @param predicate the predicate that must be satisfied in order to be returned
   * @return list of all applications in the namespace that satisfy the specified predicate
   */
  public List<ApplicationRecord> getApps(final Id.Namespace namespace,
                                         com.google.common.base.Predicate<ApplicationRecord> predicate)
    throws Exception {
    List<ApplicationRecord> appRecords = new ArrayList<>();
    for (ApplicationSpecification appSpec : store.getAllApplications(namespace)) {
      // possible if this particular app was deploy prior to v3.2 and upgrade failed for some reason.
      ArtifactId artifactId = appSpec.getArtifactId();
      ArtifactSummary artifactSummary = artifactId == null ?
        new ArtifactSummary(appSpec.getName(), null) : ArtifactSummary.from(artifactId);
      ApplicationRecord record = new ApplicationRecord(artifactSummary, appSpec.getName(), appSpec.getDescription());
      if (predicate.apply(record)) {
        appRecords.add(record);
      }
    }

    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return Lists.newArrayList(Iterables.filter(appRecords, new com.google.common.base.Predicate<ApplicationRecord>() {
      @Override
      public boolean apply(ApplicationRecord appRecord) {
        return filter.apply(namespace.toEntityId().app(appRecord.getName()));
      }
    }));
  }

  /**
   * Get detail about the specified application
   *
   * @param appId the id of the application to get
   * @return detail about the specified application
   * @throws ApplicationNotFoundException if the specified application does not exist
   */
  public ApplicationDetail getAppDetail(Id.Application appId) throws Exception {
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }
    ensureAccess(appId.toEntityId());
    return ApplicationDetail.fromSpec(appSpec);
  }

  /**
   * Update an existing application. An application's configuration and artifact version can be updated.
   *
   * @param appId the id of the application to update
   * @param appRequest the request to update the application, including new config and artifact
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return information about the deployed application
   * @throws ApplicationNotFoundException if the specified application does not exist
   * @throws ArtifactNotFoundException if the requested artifact does not exist
   * @throws InvalidArtifactException if the specified artifact is invalid. For example, if the artifact name changed,
   *                                  if the version is an invalid version, or the artifact contains no app classes
   * @throws Exception if there was an exception during the deployment pipeline. This exception will often wrap
   *                   the actual exception
   */
  public ApplicationWithPrograms updateApp(Id.Application appId, AppRequest appRequest,
                                           ProgramTerminator programTerminator) throws Exception {

    // check that app exists
    ApplicationSpecification currentSpec = store.getApplication(appId);
    if (currentSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }
    // App exists. Check if the current user has admin privileges on it before updating. The user's write privileges on
    // the namespace will get enforced in the deployApp method.
    authorizationEnforcer.enforce(appId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    ArtifactId currentArtifact = currentSpec.getArtifactId();

    // if no artifact is given, use the current one.
    ArtifactId newArtifactId = currentArtifact;
    // otherwise, check requested artifact is valid and use it
    ArtifactSummary requestedArtifact = appRequest.getArtifact();
    if (requestedArtifact != null) {
      // cannot change artifact name, only artifact version.
      if (!currentArtifact.getName().equals(requestedArtifact.getName())) {
        throw new InvalidArtifactException(String.format(
          " Only artifact version updates are allowed. Cannot change from artifact '%s' to '%s'.",
          currentArtifact.getName(), requestedArtifact.getName()));
      }

      if (!currentArtifact.getScope().equals(requestedArtifact.getScope())) {
        throw new InvalidArtifactException("Only artifact version updates are allowed. " +
          "Cannot change from a non-system artifact to a system artifact or vice versa.");
      }

      // check requested artifact version is valid
      ArtifactVersion requestedVersion = new ArtifactVersion(requestedArtifact.getVersion());
      if (requestedVersion.getVersion() == null) {
        throw new InvalidArtifactException(String.format(
          "Requested artifact version '%s' is invalid", requestedArtifact.getVersion()));
      }
      newArtifactId = new ArtifactId(currentArtifact.getName(), requestedVersion, currentArtifact.getScope());
    }

    Object requestedConfigObj = appRequest.getConfig();
    // if config is null, use the previous config
    String requestedConfigStr = requestedConfigObj == null ?
      currentSpec.getConfiguration() : GSON.toJson(requestedConfigObj);

    Id.Artifact artifactId = Artifacts.toArtifactId(appId.getNamespace().toEntityId(), newArtifactId).toId();
    return deployApp(appId.getNamespace(), appId.getId(), artifactId, requestedConfigStr, programTerminator);
  }

  /**
   * Deploy an application by first adding the application jar to the artifact repository, then creating an application
   * using that newly added artifact.
   *
   * @param namespace the namespace to deploy the application and artifact in
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param artifactId the id of the artifact to add and create the application from
   * @param jarFile the application jar to add as an artifact and create the application from
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return information about the deployed application
   * @throws InvalidArtifactException the the artifact is invalid. For example, if it does not contain any app classes
   * @throws ArtifactAlreadyExistsException if the specified artifact already exists
   * @throws IOException if there was an IO error writing the artifact
   */
  public ApplicationWithPrograms deployAppAndArtifact(Id.Namespace namespace, @Nullable String appName,
                                                      Id.Artifact artifactId, File jarFile,
                                                      @Nullable String configStr,
                                                      ProgramTerminator programTerminator) throws Exception {

    ArtifactDetail artifactDetail = artifactRepository.addArtifact(artifactId, jarFile);
    try {
      return deployApp(namespace.toEntityId(), appName, configStr, programTerminator, artifactDetail);
    } catch (Exception e) {
      // if we added the artifact, but failed to deploy the application, delete the artifact to bring us back
      // to the state we were in before this call.
      try {
        artifactRepository.deleteArtifact(artifactId);
      } catch (IOException e2) {
        // if the delete fails, nothing we can do, just log it and continue on
        LOG.warn("Failed to delete artifact {} after deployment of artifact and application failed.", artifactId, e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed, the Application
   * class is instantiated and configure() is called in order to generate an {@link ApplicationSpecification}.
   * Programs, datasets, and streams are created based on the specification before the spec is persisted in the
   * {@link Store}. This method can create a new application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param artifactId the id of the artifact to create the application from
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws Exception if there was an exception during the deployment pipeline. This exception will often wrap
   *                   the actual exception
   */
  public ApplicationWithPrograms deployApp(Id.Namespace namespace, @Nullable String appName,
                                           Id.Artifact artifactId,
                                           @Nullable String configStr,
                                           ProgramTerminator programTerminator) throws Exception {
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    return deployApp(namespace.toEntityId(), appName, configStr, programTerminator, artifactDetail);
  }

  /**
   * Remove all the applications inside the given {@link Id.Namespace}
   *
   * @param namespaceId the {@link Id.Namespace} under which all application should be deleted
   * @throws Exception
   */
  public void removeAll(final Id.Namespace namespaceId) throws Exception {
    List<ApplicationSpecification> allSpecs = new ArrayList<>(
      store.getAllApplications(namespaceId));
    //Check if any program associated with this namespace is running
    Iterable<ProgramRuntimeService.RuntimeInfo> runtimeInfos =
      Iterables.filter(runtimeService.listAll(ProgramType.values()),
                       new com.google.common.base.Predicate<ProgramRuntimeService.RuntimeInfo>() {
      @Override
      public boolean apply(ProgramRuntimeService.RuntimeInfo runtimeInfo) {
        return runtimeInfo.getProgramId().toEntityId().getNamespace().equals(namespaceId.getId());
      }
    });

    Set<String> runningPrograms = new HashSet<>();
    for (ProgramRuntimeService.RuntimeInfo runtimeInfo : runtimeInfos) {
      runningPrograms.add(runtimeInfo.getProgramId().toEntityId().getApplication() +
                            ": " + runtimeInfo.getProgramId().toEntityId().getProgram());
    }

    if (!runningPrograms.isEmpty()) {
      String appAllRunningPrograms = Joiner.on(',')
        .join(runningPrograms);
      throw new CannotBeDeletedException(namespaceId,
                                         "The following programs are still running: " + appAllRunningPrograms) {
        //Keeping this for backward compatibility. Ideally this should return conflict, not forbidden.
        @Override
        public int getStatusCode() {
          return HttpResponseStatus.FORBIDDEN.getCode();
        }
      };
    }
    //All Apps are STOPPED, delete them
    for (ApplicationSpecification appSpec : allSpecs) {
      Id.Application id = Id.Application.from(namespaceId.getId(), appSpec.getName());
      deleteApp(id, appSpec);
    }
  }

  /**
   * Delete an application specified by appId.
   *
   * @param appId the {@link Id.Application} of the application to be removed
   * @throws Exception
   */
  public void removeApplication(final Id.Application appId) throws Exception {
    //Check if all are stopped.
    Iterable<ProgramRuntimeService.RuntimeInfo> runtimeInfos =
      Iterables.filter(runtimeService.listAll(ProgramType.values()),
                       new com.google.common.base.Predicate<ProgramRuntimeService.RuntimeInfo>() {
      @Override
      public boolean apply(ProgramRuntimeService.RuntimeInfo runtimeInfo) {
        return runtimeInfo.getProgramId().toEntityId().getApplication().equals(appId.getId());
      }
    });

    Set<String> runningPrograms = new HashSet<>();
    for (ProgramRuntimeService.RuntimeInfo runtimeInfo : runtimeInfos) {
      runningPrograms.add(runtimeInfo.getProgramId().toEntityId().getProgram());
    }

    if (!runningPrograms.isEmpty()) {
      String appAllRunningPrograms = Joiner.on(',')
        .join(runningPrograms);
      throw new CannotBeDeletedException(appId,
                                         "The following programs are still running: " + appAllRunningPrograms) {
          //Keeping this for backward compatibility. Ideally this should return conflict, not forbidden.
          @Override
          public int getStatusCode() {
            return HttpResponseStatus.FORBIDDEN.getCode();
          }
      };
    }

    ApplicationSpecification spec = store.getApplication(appId);
    if (spec == null) {
      throw new NotFoundException(appId);
    }
    deleteApp(appId, spec);
  }

  /**
   * Get detail about the plugin in the specified application
   *
   * @param appId the id of the application
   * @return list of plugins in the application
   * @throws ApplicationNotFoundException if the specified application does not exist
   */
  public List<PluginInstanceDetail> getPlugins(ApplicationId appId)
    throws ApplicationNotFoundException {
    ApplicationSpecification appSpec = store.getApplication(appId.toId());
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId.toId());
    }
    List<PluginInstanceDetail> pluginInstanceDetails = new ArrayList<>();
    for (Map.Entry<String, Plugin> entry : appSpec.getPlugins().entrySet()) {
      pluginInstanceDetails.add(new PluginInstanceDetail(entry.getKey(), entry.getValue()));
    }
    return pluginInstanceDetails;
  }

  private Iterable<ProgramSpecification> getProgramSpecs(Id.Application appId) {
    ApplicationSpecification appSpec = store.getApplication(appId);
    return Iterables.concat(appSpec.getFlows().values(),
                            appSpec.getMapReduce().values(),
                            appSpec.getServices().values(),
                            appSpec.getSpark().values(),
                            appSpec.getWorkers().values(),
                            appSpec.getWorkflows().values());
  }

  /**
   * Delete the metrics for an application, or if null is provided as the application ID, for all apps.
   *
   * @param applicationId the application to delete metrics for.
   * If null, metrics for all applications in the namespace are deleted.
   */
  private void deleteMetrics(String namespaceId, String applicationId) throws Exception {
    Collection<ApplicationSpecification> applications = Lists.newArrayList();
    if (applicationId == null) {
      applications = this.store.getAllApplications(new Id.Namespace(namespaceId));
    } else {
      ApplicationSpecification spec = this.store.getApplication
        (new Id.Application(new Id.Namespace(namespaceId), applicationId));
      applications.add(spec);
    }

    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId);
    for (ApplicationSpecification application : applications) {
      // add or replace application name in the tagMap
      tags.put(Constants.Metrics.Tag.APP, application.getName());
      MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, tags);
      metricStore.delete(deleteQuery);
    }
  }

  /**
   * Delete stored Preferences of the application and all its programs.
   *
   * @param appId applicationId
   */
  private void deletePreferences(Id.Application appId) {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    for (ProgramSpecification spec : programSpecs) {

      preferencesStore.deleteProperties(appId.getNamespaceId(), appId.getId(),
                                        ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
      LOG.trace("Deleted Preferences of Program : {}, {}, {}, {}", appId.getNamespaceId(), appId.getId(),
                ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
    }
    preferencesStore.deleteProperties(appId.getNamespaceId(), appId.getId());
    LOG.trace("Deleted Preferences of Application : {}, {}", appId.getNamespaceId(), appId.getId());
  }

  private ApplicationWithPrograms deployApp(NamespaceId namespaceId, @Nullable String appName,
                                            @Nullable String configStr,
                                            ProgramTerminator programTerminator,
                                            ArtifactDetail artifactDetail) throws Exception {
    // Enforce that the current principal has write access to the namespace the app is being deployed to
    authorizationEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), Action.WRITE);

    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null);
    if (appClass == null) {
      throw new InvalidArtifactException(String.format("No application class found in artifact '%s' in namespace '%s'.",
                                                       artifactDetail.getDescriptor().getArtifactId(), namespaceId));
    }

    // deploy application with newly added artifact
    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactDetail.getDescriptor(), namespaceId,
                                                             appClass.getClassName(), appName, configStr);

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(programTerminator);
    // TODO: (CDAP-3258) Manager needs MUCH better error handling.
    ApplicationWithPrograms applicationWithPrograms = manager.deploy(deploymentInfo).get();
    // Deployment successful. Grant all privileges on this app to the current principal.
    authorizerInstantiator.get().grant(applicationWithPrograms.getApplicationId(),
                                       authenticationContext.getPrincipal(), ImmutableSet.of(Action.ALL));
    return applicationWithPrograms;
  }

  // deletes without performs checks that no programs are running

  /**
   * Delete the specified application without performing checks that its programs are stopped.
   *
   * @param appId the id of the application to delete
   * @param spec the spec of the application to delete
   * @throws Exception
   */
  private void deleteApp(final Id.Application appId, ApplicationSpecification spec) throws Exception {
    // enfore ADMIN privileges on the app
    authorizationEnforcer.enforce(appId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
    // first remove all privileges on the app
    revokePrivileges(appId.toEntityId(), spec);

    //Delete the schedules
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      Id.Program workflowProgramId = Id.Program.from(appId, ProgramType.WORKFLOW, workflowSpec.getName());
      scheduler.deleteSchedules(workflowProgramId, SchedulableProgramType.WORKFLOW);
    }

    deleteMetrics(appId.getNamespaceId(), appId.getId());

    //Delete all preferences of the application and of all its programs
    deletePreferences(appId);

    // Delete all streams and queues state of each flow
    // TODO: This should be unified with the DeletedProgramHandlerStage
    for (FlowSpecification flowSpecification : spec.getFlows().values()) {
      Id.Program flowProgramId = Id.Program.from(appId, ProgramType.FLOW, flowSpecification.getName());

      // Collects stream name to all group ids consuming that stream
      final Multimap<String, Long> streamGroups = HashMultimap.create();
      for (FlowletConnection connection : flowSpecification.getConnections()) {
        if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
          long groupId = FlowUtils.generateConsumerGroupId(flowProgramId, connection.getTargetName());
          streamGroups.put(connection.getSourceName(), groupId);
        }
      }
      // Remove all process states and group states for each stream
      final String namespace = String.format("%s.%s", flowProgramId.getApplicationId(), flowProgramId.getId());

      final Id.Flow flowId = Id.Flow.from(appId, flowSpecification.getName());
      impersonator.doAs(new NamespaceId(appId.getNamespaceId()), new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
            streamConsumerFactory.dropAll(Id.Stream.from(appId.getNamespaceId(), entry.getKey()),
                                          namespace, entry.getValue());
          }
          queueAdmin.dropAllForFlow(flowId);
          return null;
        }
      });
    }

    ApplicationSpecification appSpec = store.getApplication(appId);
    deleteAppMetadata(appId, appSpec);

    store.removeApplication(appId);

    try {
      usageRegistry.unregister(appId);
    } catch (Exception e) {
      LOG.warn("Failed to unregister usage of app: {}", appId, e);
    }
  }

  // TODO: CDAP-5427 - This should be a single operation
  private void revokePrivileges(ApplicationId appId, ApplicationSpecification appSpec) throws Exception {
    for (ProgramId programId : getAllPrograms(appId, appSpec)) {
      authorizerInstantiator.get().revoke(programId);
    }
    authorizerInstantiator.get().revoke(appId);
  }

  /**
   * Delete the metadata for the application and the programs.
   */
  private void deleteAppMetadata(Id.Application appId, ApplicationSpecification appSpec) {
    // Remove metadata for the Application itself.
    metadataStore.removeMetadata(appId);

    // Remove metadata for the programs of the Application
    // TODO: Need to remove this we support prefix search of metadata type.
    // See https://issues.cask.co/browse/CDAP-3669
    for (ProgramId programId : getAllPrograms(appId.toEntityId(), appSpec)) {
      metadataStore.removeMetadata(programId.toId());
    }
  }

  private Set<ProgramId> getAllPrograms(ApplicationId appId, ApplicationSpecification appSpec) {
    Set<ProgramId> result = new HashSet<>();
    Map<ProgramType, Set<String>> programTypeToNames = new HashMap<>();
    programTypeToNames.put(ProgramType.FLOW, appSpec.getFlows().keySet());
    programTypeToNames.put(ProgramType.MAPREDUCE, appSpec.getMapReduce().keySet());
    programTypeToNames.put(ProgramType.WORKFLOW, appSpec.getWorkflows().keySet());
    programTypeToNames.put(ProgramType.SERVICE, appSpec.getServices().keySet());
    programTypeToNames.put(ProgramType.SPARK, appSpec.getSpark().keySet());
    programTypeToNames.put(ProgramType.WORKER, appSpec.getWorkers().keySet());

    for (Map.Entry<ProgramType, Set<String>> entry : programTypeToNames.entrySet()) {
      Set<String> programNames = entry.getValue();
      for (String programName : programNames) {
        result.add(Ids.namespace(appId.getNamespace())
                     .app(appId.getApplication())
                     .program(entry.getKey(), programName));
      }
    }
    return result;
  }

  // get filter for app specs by artifact name and version. if they are null, it means don't filter.
  private com.google.common.base.Predicate<ApplicationRecord> getAppPredicate(Set<String> artifactNames,
                                                       @Nullable String artifactVersion) {
    if (artifactNames.isEmpty() && artifactVersion == null) {
      return Predicates.alwaysTrue();
    } else if (artifactNames.isEmpty()) {
      return new ArtifactVersionPredicate(artifactVersion);
    } else if (artifactVersion == null) {
      return new ArtifactNamesPredicate(artifactNames);
    } else {
      return Predicates.and(new ArtifactNamesPredicate(artifactNames), new ArtifactVersionPredicate(artifactVersion));
    }
  }

  /**
   * Ensures that the logged-in user has a {@link Action privilege} on the specified dataset instance.
   *
   * @param appId the {@link ApplicationId} to check for privileges
   * @throws UnauthorizedException if the logged in user has no {@link Action privileges} on the specified dataset
   */
  private void ensureAccess(ApplicationId appId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!Principal.SYSTEM.equals(principal) && !filter.apply(appId)) {
      throw new UnauthorizedException(principal, appId);
    }
  }

  /**
   * Returns true if the application artifact is in a whitelist of names
   */
  private static class ArtifactNamesPredicate implements com.google.common.base.Predicate<ApplicationRecord> {
    private final Set<String> names;

    ArtifactNamesPredicate(Set<String> names) {
      this.names = names;
    }

    @Override
    public boolean apply(ApplicationRecord input) {
      return names.contains(input.getArtifact().getName());
    }
  }

  /**
   * Returns true if the application artifact is a specific version
   */
  private static class ArtifactVersionPredicate implements com.google.common.base.Predicate<ApplicationRecord> {
    private final String version;

    ArtifactVersionPredicate(String version) {
      this.version = version;
    }

    @Override
    public boolean apply(ApplicationRecord input) {
      return version.equals(input.getArtifact().getVersion());
    }
  }
}
