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
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.CannotBeDeletedException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.InMemoryConfigurator;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationDeployScope;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterDeploymentInfo;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.templates.AdapterSpecification;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.FileUtils;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Adapters.
 */
public class AdapterService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> templateManagerFactory;
  private final ManagerFactory<AdapterDeploymentInfo, AdapterSpecification> adapterManagerFactory;
  private final CConfiguration configuration;
  private final Scheduler scheduler;
  private final Store store;
  private final NamespacedLocationFactory namespacedLocationFactory;
  // template name to template info mapping
  private Map<String, ApplicationTemplateInfo> appTemplateInfos;
  // jar file name to template info mapping
  private Map<String, ApplicationTemplateInfo> fileToTemplateMap;

  @Inject
  public AdapterService(CConfiguration configuration, Scheduler scheduler, Store store,
                        @Named("templates")
                        ManagerFactory<DeploymentInfo, ApplicationWithPrograms> templateManagerFactory,
                        @Named("adapters")
                        ManagerFactory<AdapterDeploymentInfo, AdapterSpecification> adapterManagerFactory,
                        NamespacedLocationFactory namespacedLocationFactory) {
    this.configuration = configuration;
    this.scheduler = scheduler;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.store = store;
    this.templateManagerFactory = templateManagerFactory;
    this.adapterManagerFactory = adapterManagerFactory;
    this.appTemplateInfos = Maps.newHashMap();
    this.fileToTemplateMap = Maps.newHashMap();
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
   * Deploy the given application template in the given namespace.
   *
   * @param namespace the namespace to deploy in
   * @param templateName the name of the template to deploy
   * @throws NotFoundException if the template was not found
   * @throws IllegalArgumentException if the template is invalid
   * @throws IOException if there was an error reading the template jar
   * @throws TimeoutException if there was a timeout examining the template jar
   */
  public void deployTemplate(Id.Namespace namespace, String templateName)
    throws NotFoundException, InterruptedException, ExecutionException, TimeoutException, IOException {
    ApplicationTemplateInfo templateInfo = appTemplateInfos.get(templateName);
    if (templateInfo == null) {
      throw new NotFoundException(Id.ApplicationTemplate.from(templateName));
    }
    // make sure we're up to date on template info
    registerTemplates();
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
    return appTemplateInfos.get(templateName);
  }

  /**
   * Retrieves the {@link AdapterConfig} specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested {@link AdapterConfig} or null if no such AdapterInfo exists
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterSpecification getAdapter(Id.Namespace namespace, String adapterName) throws AdapterNotFoundException {
    AdapterSpecification adapterSpec = store.getAdapter(namespace, adapterName);
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

  /**
   * Sets the status of an Adapter specified by the name in a given namespace.
   *
   * @param namespace namespace of the adapter
   * @param adapterName name of the adapter
   * @return specified Adapter's previous status
   * @throws AdapterNotFoundException if the specified adapter is not found
   */
  public AdapterStatus setAdapterStatus(Id.Namespace namespace, String adapterName, AdapterStatus status)
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
  public Collection<AdapterSpecification> getAdapters(Id.Namespace namespace) {
    return store.getAllAdapters(namespace);
  }

  /**
   * Retrieves an Collection of {@link AdapterConfig} in a given namespace that use the given template.
   *
   * @param namespace namespace to lookup the adapter
   * @param template the template of requested adapters
   * @return Collection of requested {@link AdapterConfig}
   */
  public Collection<AdapterSpecification> getAdapters(Id.Namespace namespace, final String template) {
    // Alternative is to construct the key using adapterType as well, when storing the the adapterSpec. That approach
    // will make lookup by adapterType simpler, but it will increase the complexity of lookup by namespace + adapterName
    List<AdapterSpecification> adaptersByType = Lists.newArrayList();
    Collection<AdapterSpecification> adapters = store.getAllAdapters(namespace);
    for (AdapterSpecification adapterSpec : adapters) {
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
   * @throws IllegalArgumentException on other input errors.
   */
  public void createAdapter(Id.Namespace namespace, String adapterName, AdapterConfig adapterConfig)
    throws IllegalArgumentException, AdapterAlreadyExistsException {

    ApplicationTemplateInfo applicationTemplateInfo = appTemplateInfos.get(adapterConfig.getTemplate());
    Preconditions.checkArgument(applicationTemplateInfo != null,
                                "Applicaiton template %s not found", adapterConfig.getTemplate());

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
   * Remove adapter identified by the namespace and name.
   *
   * @param namespace namespace id
   * @param adapterName adapter name
   * @throws AdapterNotFoundException if the adapter to be removed is not found.
   * @throws CannotBeDeletedException if the adapter is not stopped.
   */
  public void removeAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, CannotBeDeletedException {

    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    if (adapterStatus != AdapterStatus.STOPPED) {
      throw new CannotBeDeletedException(Id.Adapter.from(namespace, adapterName));
    }
    store.removeAdapter(namespace, adapterName);

    // TODO: Delete the application if this is the last adapter
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
  public void stopAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, InvalidAdapterOperationException, SchedulerException {

    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    if (AdapterStatus.STOPPED.equals(adapterStatus)) {
      throw new InvalidAdapterOperationException("Adapter is already stopped.");
    }

    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName);

    ProgramType programType = appTemplateInfos.get(adapterSpec.getTemplate()).getProgramType();
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
  }

  /**
   * Start the given adapter. Creates a schedule for a workflow adapter and starts the worker for a worker adapter.
   *
   * @param namespace the namespace the adapter is deployed in
   * @param adapterName the name of the adapter
   * @throws NotFoundException if the adapter could not be found
   * @throws InvalidAdapterOperationException if the adapter is already started
   * @throws SchedulerException if there was some error creating the schedule for the adapter
   */
  public void startAdapter(Id.Namespace namespace, String adapterName)
    throws NotFoundException, InvalidAdapterOperationException, SchedulerException {
    AdapterStatus adapterStatus = getAdapterStatus(namespace, adapterName);
    if (AdapterStatus.STARTED.equals(adapterStatus)) {
      throw new InvalidAdapterOperationException("Adapter is already started.");
    }

    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName);

    ProgramType programType = appTemplateInfos.get(adapterSpec.getTemplate()).getProgramType();
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

  private void startWorkflowAdapter(Id.Namespace namespace, AdapterSpecification adapterSpec)
    throws NotFoundException, SchedulerException {
    String workflowName = adapterSpec.getScheduleSpec().getProgram().getProgramName();
    Id.Program workflowId = Id.Program.from(namespace.getId(), adapterSpec.getTemplate(),
                                            ProgramType.WORKFLOW, workflowName);

    ScheduleSpecification scheduleSpec = adapterSpec.getScheduleSpec();

    scheduler.schedule(workflowId, scheduleSpec.getProgram().getProgramType(), scheduleSpec.getSchedule());
    //TODO: Scheduler API should also manage the MDS.
    store.addSchedule(workflowId, scheduleSpec);
  }

  private void stopWorkflowAdapter(Id.Namespace namespace, AdapterSpecification adapterSpec)
    throws NotFoundException, SchedulerException {
    String workflowName = adapterSpec.getScheduleSpec().getProgram().getProgramName();
    Id.Program workflowId = Id.Program.from(namespace.getId(), adapterSpec.getTemplate(),
                                            ProgramType.WORKFLOW, workflowName);

    String scheduleName = adapterSpec.getScheduleSpec().getSchedule().getName();
    scheduler.deleteSchedule(workflowId, SchedulableProgramType.WORKFLOW, scheduleName);
    //TODO: Scheduler API should also manage the MDS.
    store.deleteSchedule(workflowId, SchedulableProgramType.WORKFLOW, scheduleName);
  }

  private void startWorkerAdapter(Id.Namespace namespace, AdapterSpecification adapterSpec) {
    // TODO: implement
  }

  private void stopWorkerAdapter(Id.Namespace namespace, AdapterSpecification adapterSpec) {
    // TODO: implement
  }

  // deploys an adapter. This will call configureAdapter() on the adapter's template. It may create
  // datasets and streams. At the end it will write to the store with the adapter spec.
  private AdapterSpecification deployAdapter(Id.Namespace namespace, String adapterName,
                                             ApplicationTemplateInfo applicationTemplateInfo,
                                             ApplicationSpecification templateSpec, AdapterConfig adapterConfig) {

    Manager<AdapterDeploymentInfo, AdapterSpecification> manager = adapterManagerFactory.create(
      new ProgramTerminator() {
        @Override
        public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
          // no-op
        }
      });

    try {
      AdapterDeploymentInfo deploymentInfo = new AdapterDeploymentInfo(
        adapterConfig, applicationTemplateInfo, templateSpec);
      Location namespaceHomeLocation = namespacedLocationFactory.get(namespace);
      if (!namespaceHomeLocation.exists()) {
        String msg = String.format("Home directory %s for namespace %s not found",
                                   namespaceHomeLocation.toURI().getPath(), namespace);
        LOG.error(msg);
        throw new FileNotFoundException(msg);
      }

      return manager.deploy(namespace, adapterName, deploymentInfo).get();
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
        public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
          // no-op
        }
      });

      DeploymentInfo deploymentInfo = new DeploymentInfo(
        applicationTemplateInfo.getFile(),
        getTemplateTempLoc(namespace, applicationTemplateInfo),
        ApplicationDeployScope.SYSTEM);
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
    Location destination = namespaceHomeLocation.append(appFabricDir)
      .append(Constants.ARCHIVE_DIR).append(templateInfo.getFile().getName());
    return destination;
  }

  // Reads all the jars from the adapter directory and sets up required internal structures.
  @VisibleForTesting
  void registerTemplates() {
    try {
      // generate a completely new map in case some templates were removed
      Map<String, ApplicationTemplateInfo> newInfoMap = Maps.newHashMap();
      Map<String, ApplicationTemplateInfo> newFileTemplateMap = Maps.newHashMap();

      File baseDir = new File(configuration.get(Constants.AppFabric.APP_TEMPLATE_DIR));
      Collection<File> files = FileUtils.listFiles(baseDir, new String[]{"jar"}, true);
      for (File file : files) {
        try {
          ApplicationTemplateInfo info = getTemplateInfo(file);
          newInfoMap.put(info.getName(), info);
          newFileTemplateMap.put(info.getFile().getName(), info);
        } catch (IllegalArgumentException e) {
          LOG.error("Application template from file {} in invalid. Skipping it.", file.getName(), e);
        }
      }
      appTemplateInfos = newInfoMap;
      fileToTemplateMap = newFileTemplateMap;
    } catch (Exception e) {
      LOG.warn("Unable to read the plugins directory");
    }
  }

  private ApplicationTemplateInfo getTemplateInfo(File jarFile)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ApplicationTemplateInfo existing = fileToTemplateMap.get(jarFile.getAbsolutePath());
    HashCode fileHash = Files.hash(jarFile, Hashing.md5());
    // if the file is the same, just return
    if (existing != null && fileHash.equals(existing.getFileHash())) {
      return existing;
    }

    // instantiate the template application and call configure() on it to determine it's specification
    InMemoryConfigurator configurator = new InMemoryConfigurator(new LocalLocationFactory().create(jarFile.toURI()));
    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(2, TimeUnit.MINUTES);
    ApplicationSpecification spec = GSON.fromJson(response.get(), ApplicationSpecification.class);

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
