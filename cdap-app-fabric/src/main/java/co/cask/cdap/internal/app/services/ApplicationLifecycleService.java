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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.program.Programs;
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
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.WriteConflictException;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Service that manage lifecycle of Applications
 * TODO: Currently this only handles the deployment and deletion of the application.
 * The code from {@link AppLifecycleHttpHandler} should be moved here and the calls should be delegated to this class.
 */
public class ApplicationLifecycleService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationLifecycleService.class);
  private static final Gson GSON = new Gson();
  // for upgrades only
  private static final ProgramTerminator NO_OP_TERMINATOR = new ProgramTerminator() {
    @Override
    public void stop(Id.Program programId) throws Exception {
      // no-op
    }
  };

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

  @Inject
  public ApplicationLifecycleService(ProgramRuntimeService runtimeService, Store store, CConfiguration configuration,
                                     Scheduler scheduler, QueueAdmin queueAdmin,
                                     NamespacedLocationFactory namespacedLocationFactory,
                                     StreamConsumerFactory streamConsumerFactory, UsageRegistry usageRegistry,
                                     PreferencesStore preferencesStore, MetricStore metricStore,
                                     ArtifactRepository artifactRepository,
                                     ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory) {
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
    Id.Artifact currentArtifact = currentSpec.getArtifactId();

    // if no artifact is given, use the current one.
    Id.Artifact newArtifactId = currentArtifact;
    // otherwise, check requested artifact is valid and use it
    ArtifactSummary requestedArtifact = appRequest.getArtifact();
    if (requestedArtifact != null) {
      // cannot change artifact name, only artifact version.
      if (!currentArtifact.getName().equals(requestedArtifact.getName())) {
        throw new InvalidArtifactException(String.format(
          " Only artifact version updates are allowed. Cannot change from artifact '%s' to '%s'.",
          currentArtifact.getName(), requestedArtifact.getName()));
      }

      if (currentArtifact.getNamespace().equals(Id.Namespace.SYSTEM) != requestedArtifact.isSystem()) {
        throw new InvalidArtifactException("Only artifact version updates are allowed. " +
          "Cannot change from a non-system artifact to a system artifact or vice versa.");
      }

      // check requested artifact version is valid
      ArtifactVersion requestedVersion = new ArtifactVersion(requestedArtifact.getVersion());
      if (requestedVersion.getVersion() == null) {
        throw new InvalidArtifactException(String.format(
          "Requested artifact version '%s' is invalid", requestedArtifact.getVersion()));
      }
      newArtifactId = Id.Artifact.from(currentArtifact.getNamespace(), currentArtifact.getName(), requestedVersion);
    }

    Object requestedConfigObj = appRequest.getConfig();
    // if config is null, use the previous config
    String requestedConfigStr = requestedConfigObj == null ?
      currentSpec.getConfiguration() : GSON.toJson(requestedConfigObj);

    return deployApp(appId.getNamespace(), appId.getId(), newArtifactId, requestedConfigStr, programTerminator);
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
   * @throws WriteConflictException if there was a write conflict adding the artifact. Should be a transient error
   * @throws InvalidArtifactException the the artifact is invalid. For example, if it does not contain any app classes
   * @throws ArtifactAlreadyExistsException if the specified artifact already exists
   * @throws IOException if there was an IO error writing the artifact
   */
  public ApplicationWithPrograms deployAppAndArtifact(Id.Namespace namespace, @Nullable String appName,
                                                      Id.Artifact artifactId, File jarFile,
                                                      @Nullable String configStr,
                                                      ProgramTerminator programTerminator) throws Exception {

    ArtifactDetail artifactDetail = artifactRepository.addArtifact(artifactId, jarFile);
    return deployApp(namespace, appName, configStr, programTerminator, artifactDetail);
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
    return deployApp(namespace, appName, configStr, programTerminator, artifactDetail);
  }

  /**
   * Remove all the applications inside the given {@link Id.Namespace}
   *
   * @param identifier the {@link Id.Namespace} under which all application should be deleted
   * @throws Exception
   */
  public void removeAll(Id.Namespace identifier) throws Exception {
    List<ApplicationSpecification> allSpecs = new ArrayList<>(
      store.getAllApplications(identifier));

    //Check if any program associated with this namespace is running
    final Id.Namespace accId = Id.Namespace.from(identifier.getId());
    boolean appRunning = runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().getNamespace().equals(accId);
      }
    }, ProgramType.values());

    if (appRunning) {
      throw new CannotBeDeletedException(identifier, "One of the program associated with this namespace is still " +
        "running");
    }

    //All Apps are STOPPED, delete them
    for (ApplicationSpecification appSpec : allSpecs) {
      Id.Application id = Id.Application.from(identifier.getId(), appSpec.getName());
      removeApplication(id);
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
    boolean appRunning = runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().equals(appId);
      }
    }, ProgramType.values());

    if (appRunning) {
      throw new CannotBeDeletedException(appId);
    }

    ApplicationSpecification spec = store.getApplication(appId);
    if (spec == null) {
      throw new NotFoundException(appId);
    }

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
      Multimap<String, Long> streamGroups = HashMultimap.create();
      for (FlowletConnection connection : flowSpecification.getConnections()) {
        if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
          long groupId = FlowUtils.generateConsumerGroupId(flowProgramId, connection.getTargetName());
          streamGroups.put(connection.getSourceName(), groupId);
        }
      }
      // Remove all process states and group states for each stream
      String namespace = String.format("%s.%s", flowProgramId.getApplicationId(), flowProgramId.getId());
      for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
        streamConsumerFactory.dropAll(Id.Stream.from(appId.getNamespaceId(), entry.getKey()),
                                      namespace, entry.getValue());
      }

      queueAdmin.dropAllForFlow(Id.Flow.from(appId, flowSpecification.getName()));
    }
    deleteProgramLocations(appId);

    Location appArchive = store.getApplicationArchiveLocation(appId);
    Preconditions.checkNotNull(appArchive, "Could not find the location of application", appId.getId());
    if (!appArchive.delete()) {
      LOG.debug("Could not delete application archive");
    }
    store.removeApplication(appId);

    try {
      usageRegistry.unregister(appId);
    } catch (Exception e) {
      LOG.warn("Failed to unregister usage of app: {}", appId, e);
    }
  }

  /**
   * Upgrade from the previous version of CDAP to the current version. In 3.2, this means adding artifacts for all
   * existing applications and updating their specs to include the artifact id.
   */
  public void upgrade(boolean continueOnFailure) throws Exception {
    File tmpDir = new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR),
      configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!tmpDir.exists()) {
      if (!tmpDir.mkdirs()) {
        throw new IOException("Unable to create tmp dir " + tmpDir.getAbsolutePath());
      }
    }

    for (NamespaceMeta namespaceMeta : store.listNamespaces()) {
      Id.Namespace namespaceId = Id.Namespace.from(namespaceMeta.getName());
      for (ApplicationSpecification appSpec : store.getAllApplications(namespaceId)) {
        Id.Application appId = Id.Application.from(namespaceId, appSpec.getName());

        if (appSpec.getArtifactId() != null) {
          // this is possible if upgrade failed midway through. It means this app was already updated so continue on.
          continue;
        }

        Location appJarLocation = store.getApplicationArchiveLocation(appId);
        if (appJarLocation == null) {
          LOG.error(String.format("Unable to get location of jar for app '%s' in namespace '%s'. " +
              "You will need to re-deploy the app after upgrade.",
            appId.getNamespaceId(), appId.getId()));
          continue;
        }

        // copy jar to local file
        File tmpFile = File.createTempFile("tmpApp", ".jar", tmpDir);
        Files.copy(Locations.newInputSupplier(appJarLocation), tmpFile);

        String version = appSpec.getVersion();
        if (version == null || version.isEmpty()) {
          // version derived from manifest.
          JarFile jarFile = new JarFile(tmpFile);
          Manifest manifest = jarFile.getManifest();
          if (manifest != null && manifest.getMainAttributes() != null) {
            version = manifest.getMainAttributes().getValue(ManifestFields.BUNDLE_VERSION);
          }
          // If version couldn't be found through manifest, default it to 1.0.0
          if (version == null || version.isEmpty()) {
            version = "1.0.0";
          }
        }

        ArtifactDetail artifactDetail;
        // use app name as artifact name
        Id.Artifact artifactId = Id.Artifact.from(namespaceId, appId.getId(), version);
        try {
          artifactDetail = artifactRepository.addArtifact(artifactId, tmpFile);
        } catch (WriteConflictException e) {
          // this shouldn't happen since nothing else should be running
          LOG.error("Write conflict when adding artifact for app '%s' in namespace '%s'. " +
                    "Please try re-running the upgrade.");
          if (continueOnFailure) {
            continue;
          }
          throw e;
        } catch (ArtifactAlreadyExistsException e) {
          // this can happen if the upgrade tool ran already, the artifact was added, but the app metadata was not
          // updated. In that case, just look up artifact detail from the repository instead of adding the artifact,
          // and proceed to update the app metadata again
          try {
            artifactDetail = artifactRepository.getArtifact(artifactId);
          } catch (Exception e2) {
            LOG.error("Error looking up artifact detail for artifact {}. Please try re-running the upgrade.",
                      artifactId, e);
            if (continueOnFailure) {
              continue;
            }
            throw e2;
          }
        } catch (InvalidArtifactException e) {
          LOG.error("Artifact {} is invalid. You will need to redeploy the app manually after upgrade.",
                    artifactId, e);
          // this should not happen either since the app jar was successfully deployed already
          if (continueOnFailure) {
            continue;
          }
          throw e;
        }

        // app programs will not change during upgrade so we can pass a no-op terminator
        try {
          deployApp(namespaceId, appSpec.getName(), appSpec.getConfiguration(), NO_OP_TERMINATOR, artifactDetail);
        } catch (Exception e) {
          LOG.error("Error updating app metadata. Please try re-running the upgrade. If that fails, you will need " +
            "to delete and redeploy the app manually.", e);
          if (continueOnFailure) {
            continue;
          }
          throw e;
        }
      }
    }
  }

  /**
   * Delete the jar location of the program.
   *
   * @param appId applicationId.
   * @throws IOException if there are errors with location IO
   */
  private void deleteProgramLocations(Id.Application appId) throws IOException {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    for (ProgramSpecification spec : programSpecs) {
      ProgramType type = ProgramTypes.fromSpecification(spec);
      Id.Program programId = Id.Program.from(appId, type, spec.getName());
      try {
        Location location = Programs.programLocation(namespacedLocationFactory, appFabricDir, programId);
        location.delete();
      } catch (FileNotFoundException e) {
        LOG.warn("Program jar for program {} not found.", programId.toString(), e);
      }
    }

    // Delete webapp
    // TODO: this will go away once webapp gets a spec
    try {
      Id.Program programId = Id.Program.from(appId.getNamespaceId(), appId.getId(),
                                             ProgramType.WEBAPP, ProgramType.WEBAPP.name().toLowerCase());
      Location location = Programs.programLocation(namespacedLocationFactory, appFabricDir, programId);
      location.delete();
    } catch (FileNotFoundException e) {
      // expected exception when webapp is not present.
    }
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

  private ApplicationWithPrograms deployApp(Id.Namespace namespace, @Nullable String appName,
                                            @Nullable String configStr,
                                            ProgramTerminator programTerminator,
                                            ArtifactDetail artifactDetail) throws Exception {

    Id.Artifact artifactId = Id.Artifact.from(namespace, artifactDetail.getDescriptor());
    Set<ApplicationClass> appClasses = artifactDetail.getMeta().getClasses().getApps();
    if (appClasses.isEmpty()) {
      throw new InvalidArtifactException(String.format("No application classes found in artifact '%s'.", artifactId));
    }
    String className = appClasses.iterator().next().getClassName();
    Location location = artifactDetail.getDescriptor().getLocation();

    // deploy application with newly added artifact
    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactId, className, location, configStr);

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(programTerminator);
    // TODO: (CDAP-3258) Manager needs MUCH better error handling.
    return manager.deploy(namespace, appName, deploymentInfo).get();
  }
}
