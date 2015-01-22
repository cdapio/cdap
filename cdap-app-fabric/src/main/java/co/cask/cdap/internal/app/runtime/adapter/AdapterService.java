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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.Schedules;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Adapters.
 */
public class AdapterService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String DATASET_CLASS = "dataset.class";
  private final LocationFactory locationFactory;
  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final CConfiguration configuration;
  private final DatasetFramework datasetFramework;
  private final StreamAdmin streamAdmin;
  private final Scheduler scheduler;
  private final Store store;
  private Map<String, AdapterTypeInfo> adapterTypeInfos;
  private final String archiveDir;


  @Inject
  public AdapterService(CConfiguration configuration, DatasetFramework datasetFramework, Scheduler scheduler,
                        StreamAdmin streamAdmin, StoreFactory storeFactory, LocationFactory locationFactory,
                        ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory) {
    this.configuration = configuration;
    this.datasetFramework = datasetFramework;
    this.scheduler = scheduler;
    this.streamAdmin = streamAdmin;
    this.store = storeFactory.create();
    this.locationFactory = locationFactory;
    this.managerFactory = managerFactory;
    archiveDir = configuration.get(Constants.AppFabric.OUTPUT_DIR) + "/archive";
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting AdapterService");
    this.adapterTypeInfos = Maps.newHashMap();
    registerAdapters();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down AdapterService");
  }

  /**
   * Get the {@link AdapterTypeInfo} for a given adapter type.
   *
   * @param adapterType adapter type
   * @return instance of {@link AdapterTypeInfo} if available, null otherwise
   */
  @Nullable
  public AdapterTypeInfo getAdapterTypeInfo(String adapterType) {
    return this.adapterTypeInfos.get(adapterType);
  }

  /**
   * Retrieves the {@link AdapterSpecification} specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested {@link AdapterSpecification} or null if no such AdapterInfo exists
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterSpecification getAdapter(String namespace, String adapterName) throws AdapterNotFoundException {
    AdapterSpecification adapterSpecification = store.getAdapter(Id.Namespace.from(namespace), adapterName);
    if (adapterSpecification == null) {
      throw new AdapterNotFoundException(adapterName);
    }
    return adapterSpecification;
  }

  /**
   * Get all adapters in a given namespace.
   *
   * @param namespace namespace to look up the adapters
   * @return {@link Collection} of {@link AdapterSpecification}
   */
  public Collection<AdapterSpecification> getAdapters(String namespace) {
    return store.getAllAdapters(Id.Namespace.from(namespace));
  }

  /**
   * Retrieves an Iterable of {@link AdapterSpecification} specified by the adapterType in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterType type of requested adapters
   * @return Iterable of requested {@link AdapterSpecification}
   */
  public Collection<AdapterSpecification> getAdapters(String namespace, final String adapterType) {
    // Alternative is to construct the key using adapterType as well, when storing the the adapterSpec. That approach
    // will make lookup by adapterType simpler, but it will increase the complexity of lookup by namespace + adapterName
    List<AdapterSpecification> adaptersByType = Lists.newArrayList();
    Collection<AdapterSpecification> adapters = getAdapters(namespace);
    for (AdapterSpecification adapterSpec : adapters) {
      if (adapterSpec.getType().equals(adapterType)) {
        adaptersByType.add(adapterSpec);
      }
    }
    return adaptersByType;
  }

  /**
   * Creates the adapter
   * @param namespaceId namespace to create the adapter
   * @param adapterSpec specification of the adapter to create
   * @throws AdapterAlreadyExistsException if an adapter with the same name already exists.
   * @throws IllegalArgumentException on other input errors.
   */
  public void createAdapter(String namespaceId, AdapterSpecification adapterSpec)
    throws IllegalArgumentException, AdapterAlreadyExistsException {
    //TODO: ensure that this method does not catch AdapterAlreadyExistsException and pass on as RuntimeExceptions
    // (being done in another PR)

    AdapterTypeInfo adapterTypeInfo = adapterTypeInfos.get(adapterSpec.getType());
    Preconditions.checkArgument(adapterTypeInfo != null, "Adapter type %s not found", adapterSpec.getType());

    try {
      String adapterName = adapterSpec.getName();
      AdapterSpecification existingAdapter = store.getAdapter(Id.Namespace.from(namespaceId), adapterName);
      if (existingAdapter != null) {
        throw new AdapterAlreadyExistsException(adapterName);
      }

      ApplicationSpecification appSpec = deployApplication(namespaceId, adapterTypeInfo);

      validateSources(adapterName, adapterSpec.getSources());
      createSinks(adapterSpec.getSinks(), adapterTypeInfo);

      // If the adapter already exists, remove existing schedule to replace with the new one.
      AdapterSpecification existingSpec = store.getAdapter(Id.Namespace.from(namespaceId), adapterName);
      if (existingSpec != null) {
        unschedule(namespaceId, appSpec, adapterTypeInfo, existingSpec);
      }

      schedule(namespaceId, appSpec, adapterTypeInfo, adapterSpec);
      store.addAdapter(Id.Namespace.from(namespaceId), adapterSpec);

    } catch (Exception e) {
      throw new RuntimeException("Error deploying adapter application " + e.getMessage());
    }
  }

  /**
   * Remove adapter identified by the namespace and name.
   *
   * @param namespace namespace id
   * @param adapterName adapter name
   * @throws AdapterNotFoundException if the adapter to be removed is not found.
   */
  public void removeAdapter(String namespace, String adapterName) throws AdapterNotFoundException {
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName);
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespaceId, adapterSpec.getType()));
    unschedule(namespace, appSpec, adapterTypeInfos.get(adapterSpec.getType()), adapterSpec);
    store.removeAdapter(namespaceId, adapterName);

    // TODO: Delete the application if this is the last adapter
  }

  public void stopAdapter(String namespace, String adapterName) throws AdapterNotFoundException {
    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName);
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespace, adapterSpec.getType()));
    unschedule(namespace, appSpec, adapterTypeInfos.get(adapterSpec.getType()), adapterSpec);
  }

  public void startAdapter(String namespace, String adapterName) throws AdapterNotFoundException {
    AdapterSpecification adapterSpec = getAdapter(namespace, adapterName);
    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespace, adapterSpec.getType()));
    schedule(namespace, appSpec, adapterTypeInfos.get(adapterSpec.getType()), adapterSpec);
  }

  // Deploys adapter application if it is not already deployed.
  private ApplicationSpecification deployApplication(String namespaceId, AdapterTypeInfo adapterTypeInfo)
    throws Exception {

    ApplicationSpecification spec = store.getApplication(Id.Application.from(namespaceId, adapterTypeInfo.getType()));
    // Application is already deployed.
    if (spec != null) {
      return spec;
    }

    Manager<DeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
      @Override
      public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
        // no-op
      }
    });

    Location destination = locationFactory.create(archiveDir).append(namespaceId).append(adapterTypeInfo.getType());
    DeploymentInfo deploymentInfo = new DeploymentInfo(adapterTypeInfo.getFile(), destination);
    ApplicationWithPrograms applicationWithPrograms =
      manager.deploy(Id.Namespace.from(namespaceId), adapterTypeInfo.getType(), deploymentInfo).get();
    return applicationWithPrograms.getSpecification();
  }

  // Schedule all the programs needed for the adapter. Currently, only scheduling of workflow is supported.
  private void schedule(String namespaceId, ApplicationSpecification spec, AdapterTypeInfo adapterTypeInfo,
                        AdapterSpecification adapterSpec) {
    ProgramType programType = adapterTypeInfo.getProgramType();
    if (programType.equals(ProgramType.WORKFLOW)) {
      Map<String, WorkflowSpecification> workflowSpecs = spec.getWorkflows();
      for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
        Id.Program programId = Id.Program.from(namespaceId, spec.getName(), entry.getValue().getName());
        addSchedule(programId, adapterSpec);
      }
    } else {
      // Only Workflows are supported to be scheduled in the current implementation
      throw new UnsupportedOperationException(String.format("Unsupported program type %s for adapter",
                                                            programType.toString()));
    }
  }

  // Unschedule all the programs needed for the adapter. Currently, only unscheduling of workflow is supported.
  private void unschedule(String namespaceId, ApplicationSpecification spec, AdapterTypeInfo adapterTypeInfo,
                          AdapterSpecification adapterSpec) {
    ProgramType programType = adapterTypeInfo.getProgramType();
    if (programType.equals(ProgramType.WORKFLOW)) {
      Map<String, WorkflowSpecification> workflowSpecs = spec.getWorkflows();
      for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
        Id.Program programId = Id.Program.from(namespaceId, adapterSpec.getType(), entry.getValue().getName());
        deleteSchedule(programId, SchedulableProgramType.WORKFLOW,
                       constructScheduleName(programId, adapterSpec.getName()));
      }
    } else {
      // Only Workflows are supported to be unscheduled in the current implementation
      throw new UnsupportedOperationException(String.format("Unsupported program type %s for adapter",
                                                            programType.toString()));
    }
  }

  // Adds a schedule to the scheduler as well as to the appspec
  private void addSchedule(Id.Program programId, AdapterSpecification adapterSpec) {
    String cronExpr = Schedules.toCronExpr(adapterSpec.getProperties().get("frequency"));
    Preconditions.checkNotNull(cronExpr, "Frequency of running the adapter is missing. Cannot schedule program");
    String adapterName = adapterSpec.getName();
    Schedule schedule = new Schedule(constructScheduleName(programId, adapterName), getScheduleDescription(adapterName),
                                     cronExpr);
    ScheduleSpecification scheduleSpec = new ScheduleSpecification(schedule,
                                           new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, programId.getId()),
                                           adapterSpec.getProperties());

    scheduler.schedule(programId, scheduleSpec.getProgram().getProgramType(), scheduleSpec.getSchedule());
    //TODO: Scheduler API should also manage the MDS.
    store.addSchedule(programId, scheduleSpec);
  }

  // Deletes schedule from the scheduler as well as from the app spec
  private void deleteSchedule(Id.Program programId, SchedulableProgramType programType, String scheduleName) {
    scheduler.deleteSchedule(programId, programType, scheduleName);
    //TODO: Scheduler API should also manage the MDS.
    store.deleteSchedule(programId, programType, scheduleName);
  }

  // Sources for all adapters should exists before creating the adapters.
  private void validateSources(String adapterName, Set<Source> sources) throws IllegalArgumentException {
    // Ensure all sources exist
    for (Source source : sources) {
      Preconditions.checkArgument(Source.Type.STREAM.equals(source.getType()),
                                  String.format("Unknown Source type: %s", source.getType()));
      Preconditions.checkArgument(streamExists(source.getName()),
                                  String.format("Stream %s must exist during create of adapter: %s",
                                                source.getName(), adapterName));
    }
  }

  private boolean streamExists(String streamName) {
    try {
      return streamAdmin.exists(streamName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // create the required sinks for the adapters. Currently only DATASET sink type is supported.
  private void createSinks(Set<Sink> sinks, AdapterTypeInfo adapterTypeInfo) {
    // create sinks if it does not exist
    for (Sink sink : sinks) {
      Preconditions.checkArgument(Sink.Type.DATASET.equals(sink.getType()),
                                  String.format("Unknown Sink type: %s", sink.getType()));
      // add all properties that were defined in the manifest (default sink properties), override that with sink
      // properties passed while creating the sinks.
      DatasetProperties properties = DatasetProperties.builder()
        .addAll(adapterTypeInfo.getDefaultSinkProperties())
        .addAll(sink.getProperties())
        .build();
      createDataset(sink.getName(), properties.getProperties().get(DATASET_CLASS), properties);
    }
  }

  private void createDataset(String datasetName, String datasetClass, DatasetProperties properties) {
    Preconditions.checkNotNull(datasetClass, "Dataset class cannot be null");
    try {
      if (!datasetFramework.hasInstance(datasetName)) {
        datasetFramework.addInstance(datasetClass, datasetName, properties);
        LOG.debug("Dataset instance {} created with properties: {}.", datasetName, properties);
      } else {
        LOG.debug("Dataset instance {} already exists; not creating a new one.", datasetName);
      }
    } catch (DatasetManagementException e) {
      LOG.error("Error while creating dataset {}", datasetName, e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Error while creating dataset {}", datasetName, e);
      throw new RuntimeException(e);
    }
  }

  // Reads all the jars from the adapter directory and sets up required internal structures.
  @VisibleForTesting
  void registerAdapters() {
    try {
      File baseDir = new File(configuration.get(Constants.AppFabric.ADAPTER_DIR));
      Collection<File> files = FileUtils.listFiles(baseDir, new String[]{"jar"}, true);
      for (File file : files) {
        try {
          Manifest manifest = new JarFile(file.getAbsolutePath()).getManifest();
          AdapterTypeInfo adapterTypeInfo = createAdapterTypeInfo(file, manifest);
          if (adapterTypeInfo != null) {
            adapterTypeInfos.put(adapterTypeInfo.getType(), adapterTypeInfo);
          } else {
            LOG.warn("Missing required information to create adapter {}", file.getAbsolutePath());
          }
        } catch (IOException e) {
          LOG.warn(String.format("Unable to read adapter jar %s", file.getAbsolutePath()));
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to read the plugins directory");
    }
  }


  private AdapterTypeInfo createAdapterTypeInfo(File file, Manifest manifest) {
    if (manifest != null) {
      Attributes mainAttributes = manifest.getMainAttributes();

      String adapterType = mainAttributes.getValue(AdapterManifestAttributes.ADAPTER_TYPE);
      String adapterProgramType = mainAttributes.getValue(AdapterManifestAttributes.ADAPTER_PROGRAM_TYPE);
      String defaultAdapterProperties = mainAttributes.getValue(AdapterManifestAttributes.ADAPTER_PROPERTIES);

      String sourceType = mainAttributes.getValue(AdapterManifestAttributes.SOURCE_TYPE);
      String sinkType = mainAttributes.getValue(AdapterManifestAttributes.SINK_TYPE);
      String defaultSourceProperties = mainAttributes.getValue(AdapterManifestAttributes.SOURCE_PROPERTIES);
      String defaultSinkProperties = mainAttributes.getValue(AdapterManifestAttributes.SINK_PROPERTIES);

      if (adapterType != null && sourceType != null && sinkType != null && adapterProgramType != null) {
        return new AdapterTypeInfo(file, adapterType, Source.Type.valueOf(sourceType.toUpperCase()),
                                   Sink.Type.valueOf(sinkType.toUpperCase()),
                                   propertiesFromString(defaultSourceProperties),
                                   propertiesFromString(defaultSinkProperties),
                                   propertiesFromString(defaultAdapterProperties),
                                   ProgramType.valueOf(adapterProgramType.toUpperCase()));
      }
    }
    return null;
  }

  protected Map<String, String> propertiesFromString(String gsonEncodedMap) {
    Map<String, String> properties = GSON.fromJson(gsonEncodedMap, STRING_STRING_MAP_TYPE);
    return properties == null ? Maps.<String, String>newHashMap() : properties;
  }

  private static class AdapterManifestAttributes {
    private static final String ADAPTER_TYPE = "CDAP-Adapter-Type";
    private static final String ADAPTER_PROPERTIES = "CDAP-Adapter-Properties";
    private static final String ADAPTER_PROGRAM_TYPE = "CDAP-Adapter-Program-Type";
    private static final String SOURCE_TYPE = "CDAP-Source-Type";
    private static final String SINK_TYPE = "CDAP-Sink-Type";
    private static final String SOURCE_PROPERTIES = "CDAP-Source-Properties";
    private static final String SINK_PROPERTIES = "CDAP-Sink-Properties";
  }

  /**
   * @return construct a name of a schedule, given a programId and adapterName
   */
  public String constructScheduleName(Id.Program programId, String adapterName) {
    // For now, simply schedule the adapter's program with the name of the program being scheduled + name of the adapter
    return String.format("%s.%s", adapterName, programId.getId());
  }

  /**
   * @return description of the schedule, given an adapterName.
   */
  public String getScheduleDescription(String adapterName) {
    return String.format("Schedule for adapter: %s", adapterName);
  }
}
