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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.templates.AdapterSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.CannotBeDeletedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.InMemoryConfigurator;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationDeployScope;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterDeploymentInfo;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Adapters.
 */
public class AdapterService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private static final Function<RunRecordMeta, RunRecord> CONVERT_TO_RUN_RECORD =
    new Function<RunRecordMeta, RunRecord>() {
      @Override
      public RunRecord apply(RunRecordMeta input) {
        return new RunRecord(input);
      }
    };

  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> templateManagerFactory;
  private final ManagerFactory<AdapterDeploymentInfo, AdapterDefinition> adapterManagerFactory;
  private final CConfiguration configuration;
  private final Scheduler scheduler;
  private final ProgramLifecycleService lifecycleService;
  private final Store store;
  private final PropertiesResolver resolver;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final PluginRepository pluginRepository;

  // template name to template info mapping
  private final AtomicReference<Map<String, ApplicationTemplateInfo>> appTemplateInfos;
  // jar file name to template info mapping
  private final AtomicReference<Map<File, ApplicationTemplateInfo>> fileToTemplateMap;
  private final ApplicationLifecycleService applicationLifecycleService;

  @Inject
  public AdapterService(CConfiguration configuration, Scheduler scheduler, Store store,
                        @Named("templates")
                        ManagerFactory<DeploymentInfo, ApplicationWithPrograms> templateManagerFactory,
                        @Named("adapters")
                        ManagerFactory<AdapterDeploymentInfo, AdapterDefinition> adapterManagerFactory,
                        NamespacedLocationFactory namespacedLocationFactory, ProgramLifecycleService lifecycleService,
                        PropertiesResolver resolver,
                        PluginRepository pluginRepository, ApplicationLifecycleService applicationLifecycleService) {
    this.configuration = configuration;
    this.scheduler = scheduler;
    this.lifecycleService = lifecycleService;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.store = store;
    this.templateManagerFactory = templateManagerFactory;
    this.adapterManagerFactory = adapterManagerFactory;

    this.appTemplateInfos = new AtomicReference<Map<String, ApplicationTemplateInfo>>(
      new HashMap<String, ApplicationTemplateInfo>());
    this.fileToTemplateMap = new AtomicReference<Map<File, ApplicationTemplateInfo>>(
      new HashMap<File, ApplicationTemplateInfo>());
    this.resolver = resolver;
    this.pluginRepository = pluginRepository;
    this.applicationLifecycleService = applicationLifecycleService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting AdapterService");
    registerTemplates();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down AdapterService");
  }

  /**
   * Returns an immutable collection of all templates that are known to this service.
   * The returned information are ordered by the template name.
   */
  public Collection<ApplicationTemplateInfo> getAllTemplates() {
    return appTemplateInfos.get().values();
  }

  /**
   * Deploy the given application template in the given namespace.
   *
   * @param namespace the namespace to deploy in
   * @param templateName the name of the template to deploy
   * @throws NotFoundException if the template was not found
   * @throws IllegalArgumentException if the template is invalid
   * @throws IOException if there was an error reading the template jar
   * @throws TimeoutException if there was a timeout examining the template jar
   */
  public synchronized void deployTemplate(Id.Namespace namespace, String templateName)
    throws NotFoundException, InterruptedException, ExecutionException, TimeoutException, IOException {
    // make sure we're up to date on template info
    registerTemplates();
    ApplicationTemplateInfo templateInfo = appTemplateInfos.get().get(templateName);
    if (templateInfo == null) {
      throw new NotFoundException(Id.ApplicationTemplate.from(templateName));
    }
    deployTemplate(namespace, templateInfo);
  }

  /**
   * Get the {@link ApplicationTemplateInfo} for a given application template.
   *
   * @param templateName the template name
   * @return instance of {@link ApplicationTemplateInfo} if available, null otherwise
   */
  @Nullable
  public ApplicationTemplateInfo getApplicationTemplateInfo(String templateName) {
    return appTemplateInfos.get().get(templateName);
  }

  /**
   * Retrieves the {@link AdapterConfig} specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested {@link AdapterConfig} or null if no such AdapterInfo exists
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterDefinition getAdapter(Id.Namespace namespace, String adapterName) throws AdapterNotFoundException {
    AdapterDefinition adapterSpec = store.getAdapter(namespace, adapterName);
    if (adapterSpec == null) {
      throw new AdapterNotFoundException(Id.Adapter.from(namespace, adapterName));
    }
    return adapterSpec;
  }

  /**
   * Retrieves the status of an Adapter specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested Adapter's status
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterStatus getAdapterStatus(Id.Namespace namespace, String adapterName) throws AdapterNotFoundException {
    AdapterStatus adapterStatus = store.getAdapterStatus(namespace, adapterName);
    if (adapterStatus == null) {
      throw new AdapterNotFoundException(Id.Adapter.from(namespace, adapterName));
    }
    return adapterStatus;
  }

  public boolean canDeleteApp(Id.Application id) {
    Collection<AdapterDefinition> adapterSpecs = getAdapters(id.getNamespace(), id.getId());
    return adapterSpecs.isEmpty();
  }

  /**
   * Sets the status of an Adapter specified by the name in a given namespace.
   *
   * @param namespace namespace of the adapter
   * @param adapterName name of the adapter
   * @return specified Adapter's previous status
   * @throws AdapterNotFoundException if the specified adapter is not found
   */
  private AdapterStatus setAdapterStatus(Id.Namespace namespace, String adapterName, AdapterStatus status)
    throws AdapterNotFoundException {
    AdapterStatus existingStatus = store.setAdapterStatus(namespace, adapterName, status);
    if (existingStatus == null) {
      throw new AdapterNotFoundException(Id.Adapter.from(namespace, adapterName));
    }
    return existingStatus;
  }

  /**
   * Get all adapters in a given namespace.
   *
   * @param namespace the namespace to look up the adapters
   * @return {@link Collection} of {@link AdapterConfig}
   */
  public Collection<AdapterDefinition> getAdapters(Id.Namespace namespace) {
    return store.getAllAdapters(namespace);
  }

  /**
   * Retrieves an Collection of {@link AdapterConfig} in a given namespace that use the given template.
   *
   * @param namespace namespace to lookup the adapter
   * @param template the template of requested adapters
   * @return Collection of requested {@link AdapterConfig}
   */
  public Collection<AdapterDefinition> getAdapters(Id.Namespace namespace, final String template) {
    // Alternative is to construct the key using adapterType as well, when storing the the adapterSpec. That approach
    // will make lookup by adapterType simpler, but it will increase the complexity of lookup by namespace + adapterName
    List<AdapterDefinition> adaptersByType = Lists.newArrayList();
    Collection<AdapterDefinition> adapters = store.getAllAdapters(namespace);
    for (AdapterDefinition adapterSpec : adapters) {
      if (adapterSpec.getTemplate().equals(template)) {
        adaptersByType.add(adapterSpec);
      }
    }
    return adaptersByType;
  }

  /**
   * Creates an adapter.
   *
   * @param namespace namespace to create the adapter
   * @param adapterName name of the adapter to create
   * @param adapterConfig config for the adapter to create
   * @throws AdapterAlreadyExistsException if an adapter with the same name already exists.
   * @throws IllegalArgumentException if the adapter config is invalid.
   */
  public synchronized void createAdapter(Id.Namespace namespace, String adapterName, AdapterConfig adapterConfig)
    throws IllegalArgumentException, AdapterAlreadyExistsException {

    ApplicationTemplateInfo applicationTemplateInfo = appTemplateInfos.get().get(adapterConfig.getTemplate());
    Preconditions.checkArgument(applicationTemplateInfo != null,
                                "Application template %s not found", adapterConfig.getTemplate());

    if (store.getAdapter(namespace, adapterName) != null) {
      throw new AdapterAlreadyExistsException(adapterName);
    }

    // if the template has not been deployed, deploy it first
    Id.Application templateId = Id.Application.from(namespace, applicationTemplateInfo.getName());
    ApplicationSpecification appSpec = store.getApplication(templateId);
    if (appSpec == null) {
      appSpec = deployTemplate(namespace, applicationTemplateInfo);
    }

    deployAdapter(namespace, adapterName, applicationTemplateInfo, appSpec, adapterConfig);
  }

  /**
   * Remove adapter identified by the namespace and name and also deletes the template for the adapter if this
   * was the last adapter associated with it
   *
   * @param namespace namespace id
   * @param adapterName adapter name
   * @throws AdapterNotFoundException if the adapter to be removed is not found.
   * @throws CannotBeDeletedException if the adapter is not stopped.
   */
  public synchronized void removeAdapter(Id.Namespace namespace, String adapterName)
    throws AdapterNotFoundException, CannotBeDeletedException {

    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    // TODO: The logic is not transactional and there can be race that one thread is creating and the other
    // thread is deleting.
    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName);
    Id.Application applicationId = Id.Application.from(namespace, adapterSpec.getTemplate());
    if (adapterStatus != AdapterStatus.STOPPED) {
      throw new CannotBeDeletedException(Id.Adapter.from(namespace, adapterName), "The adapter has not been stopped." +
        " Please stop it first.");
    }
    store.removeAdapter(namespace, adapterName);
    try {
      deleteApp(applicationId);
    } catch (Exception e) {
      LOG.warn("Failed to delete the template {} for after deleting the last adapter {} associated with it.",
               adapterSpec.getTemplate(), adapterName);
    }
  }

  public synchronized void removeAdapters(Id.Namespace namespaceId)
    throws AdapterNotFoundException, CannotBeDeletedException {
    for (AdapterDefinition adapterDefinition : getAdapters(namespaceId)) {
      removeAdapter(namespaceId, adapterDefinition.getName());
    }
  }

  /**
   * Deletes the application (template) if there is no associated adapter using it
   * @param applicationId the {@link Id.Application} of the application (template)
   * @throws Exception if failed to delete the application
   */
  private void deleteApp(Id.Application applicationId) throws Exception {
    if (canDeleteApp(applicationId)) {
      applicationLifecycleService.removeApplication(applicationId);
    }
  }

  /**
   * Stop the given adapter. Deletes the schedule for a workflow adapter and stops the worker for a worker
   * adapter.
   *
   * @param namespace the namespace the adapter is deployed in
   * @param adapterName the name of the adapter
   * @throws NotFoundException if the adapter could not be found
   * @throws InvalidAdapterOperationException if the adapter is already stopped
   * @throws SchedulerException if there was some error deleting the schedule for the adapter
   */
  public synchronized void stopAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, InvalidAdapterOperationException, SchedulerException, ExecutionException,
    InterruptedException {

    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    if (AdapterStatus.STOPPED.equals(adapterStatus)) {
      throw new InvalidAdapterOperationException("Adapter is already stopped.");
    }

    AdapterDefinition adapterSpec = getAdapter(namespace, adapterName);

    LOG.info("Received request to stop Adapter {} in namespace {}", adapterName, namespace.getId());
    ProgramType programType = adapterSpec.getProgram().getType();
    if (programType == ProgramType.WORKFLOW) {
      stopWorkflowAdapter(namespace, adapterSpec);
    } else if (programType == ProgramType.WORKER) {
      stopWorkerAdapter(namespace, adapterSpec);
    } else {
      // this should never happen
      LOG.warn("Invalid program type {}.", programType);
      throw new InvalidAdapterOperationException("Invalid program type " + programType);
    }
    setAdapterStatus(namespace, adapterName, AdapterStatus.STOPPED);
    LOG.info("Stopped Adapter {} in namespace {}", adapterName, namespace.getId());
  }

  /**
   * Start the given adapter. Creates a schedule for a workflow adapter and starts the worker for a worker adapter.
   *
   * @param namespace the namespace the adapter is deployed in
   * @param adapterName the name of the adapter
   * @throws NotFoundException if the adapter could not be found
   * @throws InvalidAdapterOperationException if the adapter is already started
   * @throws SchedulerException if there was some error creating the schedule for the adapter
   * @throws IOException if there was some error starting worker adapter
   */
  public synchronized void startAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, InvalidAdapterOperationException, SchedulerException, IOException {
    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    AdapterDefinition adapterSpec = getAdapter(namespace, adapterName);
    ProgramType programType = adapterSpec.getProgram().getType();

    if (AdapterStatus.STARTED.equals(adapterStatus)) {
      // check if the actual program running or not.
      Id.Program program = getProgramId(namespace, adapterName);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = lifecycleService.findRuntimeInfo(program, programType);
      if (runtimeInfo != null) {
        throw new InvalidAdapterOperationException("Adapter is already started.");
      }
    }

    if (programType == ProgramType.WORKFLOW) {
      startWorkflowAdapter(namespace, adapterSpec);
    } else if (programType == ProgramType.WORKER) {
      startWorkerAdapter(namespace, adapterSpec);
    } else {
      // this should never happen
      LOG.warn("Invalid program type {}.", programType);
      throw new InvalidAdapterOperationException("Invalid program type " + programType);
    }

    setAdapterStatus(namespace, adapterName, AdapterStatus.STARTED);
  }

  /**
   * Fetch RunRecords for a given adapter.
   *
   * @param namespace namespace in which adapter is deployed
   * @param adapterName name of the adapter
   * @param status {@link ProgramRunStatus} status of the program running/completed/failed or all
   * @param start fetch run history that has started after the startTime in seconds
   * @param end fetch run history that has started before the endTime in seconds
   * @param limit max number of entries to fetch for this history call
   * @return list of {@link RunRecord}
   * @throws NotFoundException if adapter is not found
   */
  public List<RunRecord> getRuns(Id.Namespace namespace, String adapterName, ProgramRunStatus status,
                                 long start, long end, int limit) throws NotFoundException {
    Id.Program program = getProgramId(namespace, adapterName);

    return Lists.transform(store.getRuns(program, status, start, end, limit, adapterName), CONVERT_TO_RUN_RECORD);
  }

  /**
   * Fetch RunRecord for a given adapter.
   *
   * @param namespace namespace in which adapter is deployed
   * @param adapterName name of the adapter
   * @param runId run id
   * @return {@link RunRecord}
   * @throws NotFoundException if adapter is not found
   */
  public RunRecord getRun(Id.Namespace namespace, String adapterName, String runId) throws NotFoundException {
    Id.Program program = getProgramId(namespace, adapterName);
    RunRecordMeta runRecordMeta = store.getRun(program, runId);
    if (runRecordMeta != null && adapterName.equals(runRecordMeta.getAdapterName())) {
      return CONVERT_TO_RUN_RECORD.apply(runRecordMeta);
    }
    return null;
  }

  @VisibleForTesting
  private Id.Program getProgramId(Id.Namespace namespace, String adapterName) throws NotFoundException {
    return getProgramId(namespace, getAdapter(namespace, adapterName));
  }

  private Id.Program getProgramId(Id.Namespace namespace, AdapterDefinition adapterSpec) throws NotFoundException {
    Id.Application appId = Id.Application.from(namespace, adapterSpec.getTemplate());
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new NotFoundException(appId);
    }
    return adapterSpec.getProgram();
  }

  private void startWorkflowAdapter(Id.Namespace namespace, AdapterDefinition adapterSpec)
    throws NotFoundException, SchedulerException {
    Id.Program workflowId = getProgramId(namespace, adapterSpec);
    ScheduleSpecification scheduleSpec = adapterSpec.getScheduleSpecification();
    scheduler.schedule(workflowId, scheduleSpec.getProgram().getProgramType(), scheduleSpec.getSchedule(),
                       ImmutableMap.of(
                         ProgramOptionConstants.ADAPTER_NAME, adapterSpec.getName(),
                         ProgramOptionConstants.ADAPTER_SPEC, GSON.toJson(adapterSpec),
                         // hack for scheduler weirdness in unit tests, remove once CDAP-2281 is done
                         Constants.Scheduler.IGNORE_LAZY_START, String.valueOf(true)
                       ));
    //TODO: Scheduler API should also manage the MDS.
    store.addSchedule(workflowId, scheduleSpec);
  }

  private void stopWorkflowAdapter(Id.Namespace namespace, AdapterDefinition adapterSpec)
    throws NotFoundException, SchedulerException, ExecutionException, InterruptedException {
    Id.Program workflowId = getProgramId(namespace, adapterSpec);
    String scheduleName = adapterSpec.getScheduleSpecification().getSchedule().getName();
    try {
      scheduler.deleteSchedule(workflowId, SchedulableProgramType.WORKFLOW, scheduleName);
      //TODO: Scheduler API should also manage the MDS.
      store.deleteSchedule(workflowId, scheduleName);
    } catch (NotFoundException e) {
      // its possible a stop was already called and the schedule was deleted, but then there
      // was some failure stopping the active run.  In that case, the next time stop is called
      // the schedule will not be present. We don't want to fail in that scenario, so its ok if the
      // schedule was not found.
      LOG.trace("Could not delete adapter workflow schedule {} because it does not exist. Ignoring and moving on.",
                workflowId, e);
    }
    List<RunRecord> activeRuns = getRuns(namespace, adapterSpec.getName(), ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE,
                                         Integer.MAX_VALUE);
    for (RunRecord record : activeRuns) {
      lifecycleService.stopProgram(workflowId, RunIds.fromString(record.getPid()));
    }
  }

  private void startWorkerAdapter(Id.Namespace namespace, AdapterDefinition adapterSpec) throws NotFoundException,
    IOException {
    final Id.Adapter adapterId = Id.Adapter.from(namespace.getId(), adapterSpec.getName());
    final Id.Program workerId = getProgramId(namespace, adapterSpec);
    try {
      Map<String, String> sysArgs = resolver.getSystemProperties(workerId);
      Map<String, String> userArgs = resolver.getUserProperties(workerId);

      // Pass Adapter Name as a system property
      sysArgs.put(ProgramOptionConstants.ADAPTER_NAME, adapterSpec.getName());
      sysArgs.put(ProgramOptionConstants.ADAPTER_SPEC, GSON.toJson(adapterSpec));
      sysArgs.put(ProgramOptionConstants.INSTANCES, String.valueOf(adapterSpec.getInstances()));
      sysArgs.put(ProgramOptionConstants.RESOURCES, GSON.toJson(adapterSpec.getResources()));

      // Override resolved preferences with adapter worker spec properties.
      userArgs.putAll(adapterSpec.getRuntimeArgs());

      ProgramRuntimeService.RuntimeInfo runtimeInfo = lifecycleService.start(workerId, sysArgs, userArgs, false);
      final ProgramController controller = runtimeInfo.getController();
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State state, @Nullable Throwable cause) {
          if (state == ProgramController.State.COMPLETED) {
            completed();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }

        @Override
        public void completed() {
          super.completed();
          LOG.debug("Adapter {} completed", adapterId);
          store.setAdapterStatus(adapterId.getNamespace(), adapterId.getId(), AdapterStatus.STOPPED);
        }

        @Override
        public void error(Throwable cause) {
          super.error(cause);
          LOG.debug("Adapter {} stopped with error : {}", adapterId, cause);
          store.setAdapterStatus(adapterId.getNamespace(), adapterId.getId(), AdapterStatus.STOPPED);
        }

        @Override
        public void killed() {
          super.killed();
          LOG.debug("Adapter {} killed", adapterId);
          store.setAdapterStatus(adapterId.getNamespace(), adapterId.getId(), AdapterStatus.STOPPED);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    } catch (ProgramNotFoundException e) {
      throw new NotFoundException(workerId);
    }
  }

  private void stopWorkerAdapter(Id.Namespace namespace, AdapterDefinition adapterSpec) throws NotFoundException,
    ExecutionException, InterruptedException {
    final Id.Program workerId = getProgramId(namespace, adapterSpec);
    List<RunRecordMeta> runRecords = store.getRuns(workerId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE,
                                                   Integer.MAX_VALUE, adapterSpec.getName());
    RunRecordMeta adapterRun = Iterables.getFirst(runRecords, null);
    if (adapterRun != null) {
      RunId runId = RunIds.fromString(adapterRun.getPid());
      lifecycleService.stopProgram(workerId, runId);
    } else {
      LOG.warn("RunRecord not found for Adapter {} to be stopped", adapterSpec.getName());
    }
  }

  // deploys an adapter. This will call configureAdapter() on the adapter's template. It may create
  // datasets and streams. At the end it will write to the store with the adapter spec.
  private AdapterDefinition deployAdapter(Id.Namespace namespace, String adapterName,
                                             ApplicationTemplateInfo applicationTemplateInfo,
                                             ApplicationSpecification templateSpec,
                                             AdapterConfig adapterConfig) throws IllegalArgumentException {

    Manager<AdapterDeploymentInfo, AdapterDefinition> manager = adapterManagerFactory.create(
      new ProgramTerminator() {
        @Override
        public void stop(Id.Program programId) throws ExecutionException {
          // no-op
        }
      });

    AdapterDeploymentInfo deploymentInfo = new AdapterDeploymentInfo(
      adapterConfig, applicationTemplateInfo, templateSpec);

    try {
      return manager.deploy(namespace, adapterName, deploymentInfo).get();
    } catch (ExecutionException e) {
      // error handling for manager could use some work...
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw Throwables.propagate(cause);
      }
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Deploys adapter application
  private ApplicationSpecification deployTemplate(Id.Namespace namespace,
                                                  ApplicationTemplateInfo applicationTemplateInfo) {
    try {

      Manager<DeploymentInfo, ApplicationWithPrograms> manager = templateManagerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Program programId) throws ExecutionException {
          // no-op
        }
      });

      DeploymentInfo deploymentInfo = new DeploymentInfo(
        applicationTemplateInfo.getFile(),
        getTemplateTempLoc(namespace, applicationTemplateInfo),
        ApplicationDeployScope.SYSTEM, null);
      ApplicationWithPrograms appWithPrograms =
        manager.deploy(namespace, applicationTemplateInfo.getName(), deploymentInfo).get();
      return appWithPrograms.getSpecification();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Location getTemplateTempLoc(Id.Namespace namespace, ApplicationTemplateInfo templateInfo) throws IOException {

    Location namespaceHomeLocation = namespacedLocationFactory.get(namespace);
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
                                 namespaceHomeLocation.toURI().getPath(), namespace);
      LOG.error(msg);
      throw new FileNotFoundException(msg);
    }
    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    return namespaceHomeLocation.append(appFabricDir)
      .append(Constants.ARCHIVE_DIR).append(templateInfo.getFile().getName());
  }

  // Reads all the jars from the adapter directory and sets up required internal structures.
  @VisibleForTesting
  public void registerTemplates() {
    try {
      // generate a completely new map in case some templates were removed
      Map<String, ApplicationTemplateInfo> newInfoMap = Maps.newHashMap();
      Map<File, ApplicationTemplateInfo> newFileTemplateMap = Maps.newHashMap();

      File baseDir = new File(configuration.get(Constants.AppFabric.APP_TEMPLATE_DIR));
      List<File> files = DirUtils.listFiles(baseDir, "jar");
      for (File file : files) {
        try {
          ApplicationTemplateInfo info = getTemplateInfo(file);
          newInfoMap.put(info.getName(), info);
          newFileTemplateMap.put(info.getFile().getAbsoluteFile(), info);
        } catch (IllegalArgumentException e) {
          LOG.error("Application template from file {} in invalid. Skipping it.", file.getName(), e);
        }
      }
      appTemplateInfos.set(newInfoMap);
      fileToTemplateMap.set(newFileTemplateMap);

      // Always update all plugins for all template.
      // TODO: Performance improvement to only rebuild plugin information for those that changed
      pluginRepository.inspectPlugins(newInfoMap.values());
    } catch (Exception e) {
      LOG.warn("Unable to read the plugins directory", e);
    }
  }

  private ApplicationTemplateInfo getTemplateInfo(File jarFile)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ApplicationTemplateInfo existing = fileToTemplateMap.get().get(jarFile.getAbsoluteFile());
    HashCode fileHash = Files.hash(jarFile, Hashing.md5());
    // if the file is the same, just return
    if (existing != null && fileHash.equals(existing.getFileHash())) {
      return existing;
    }

    // instantiate the template application and call configure() on it to determine it's specification
    InMemoryConfigurator configurator = new InMemoryConfigurator(new LocalLocationFactory().create(jarFile.toURI()),
                                                                 null);
    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(2, TimeUnit.MINUTES);
    InputSupplier<? extends Reader> configSupplier = response.get();
    if (response.getExitCode() != 0 || configSupplier == null) {
      throw new IllegalArgumentException("Failed to get template info");
    }
    ApplicationSpecification spec;
    try (Reader configReader = configSupplier.getInput()) {
      spec = GSON.fromJson(configReader, ApplicationSpecification.class);
    }

    // verify that the name is ok
    Id.Application.from(Constants.DEFAULT_NAMESPACE_ID, spec.getName());

    // determine the program type of the template
    ProgramType programType;
    int numWorkflows = spec.getWorkflows().size();
    int numWorkers = spec.getWorkers().size();
    if (numWorkers == 0 && numWorkflows == 1) {
      programType = ProgramType.WORKFLOW;
    } else if (numWorkers == 1 && numWorkflows == 0) {
      programType = ProgramType.WORKER;
    } else {
      throw new IllegalArgumentException("An application template must contain exactly one worker or one workflow.");
    }

    return new ApplicationTemplateInfo(jarFile, spec.getName(), spec.getDescription(), programType, fileHash);
  }
}
