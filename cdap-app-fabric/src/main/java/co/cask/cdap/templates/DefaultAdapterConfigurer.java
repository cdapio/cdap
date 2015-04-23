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

package co.cask.cdap.templates;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.adapter.PluginRepository;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Default configurer for adapters.
 */
public class DefaultAdapterConfigurer implements AdapterConfigurer {

  private final AdapterConfig adapterConfig;
  private final Id.Program programId;
  private final PluginRepository pluginRepository;
  private final PluginInstantiator pluginInstantiator;
  private final Map<String, StreamSpecification> streams = Maps.newHashMap();
  private final Map<String, String> dataSetModules = Maps.newHashMap();
  private final Map<String, DatasetCreationSpec> dataSetInstances = Maps.newHashMap();
  private final Map<String, String> runtimeArgs = Maps.newHashMap();
  private final Map<String, AdapterPlugin> adapterPlugins = Maps.newHashMap();
  private final ApplicationSpecification templateSpec;
  private final String adapterName;
  private Schedule schedule;
  // only used if the adapter is using workers (i.e. schedule is null).
  private int instances;
  private Resources resources;

  // passed app to be used to resolve default name and description
  public DefaultAdapterConfigurer(Id.Namespace namespaceId, String adapterName, AdapterConfig adapterConfig,
                                  ApplicationSpecification templateSpec,
                                  PluginRepository pluginRepository, PluginInstantiator pluginInstantiator) {
    this.adapterName = adapterName;
    this.programId = getProgramId(namespaceId, templateSpec);
    this.adapterConfig = adapterConfig;
    this.pluginRepository = pluginRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.templateSpec = templateSpec;
    // defaults
    this.resources = new Resources();
    this.instances = 1;
  }

  /**
   * Gets the program Id of the app template for this adapter.
   */
  private Id.Program getProgramId(Id.Namespace namespaceId, ApplicationSpecification templateSpec) {
    // Get the program spec. Either one should be non-null, but not both. Also, both cannot be null.
    // It was verified during the template deployment time.
    ProgramSpecification programSpec = Objects.firstNonNull(
      Iterables.getFirst(templateSpec.getWorkers().values(), null),
      Iterables.getFirst(templateSpec.getWorkflows().values(), null)
    );

    ProgramType programType = (programSpec instanceof WorkerSpecification) ? ProgramType.WORKER : ProgramType.WORKFLOW;
    return Id.Program.from(namespaceId, templateSpec.getName(), programType, programSpec.getName());
  }

  @Override
  public void addStream(Stream stream) {
    Preconditions.checkArgument(stream != null, "Stream cannot be null.");
    StreamSpecification spec = stream.configure();
    streams.put(spec.getName(), spec);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    Preconditions.checkArgument(moduleName != null, "Dataset module name cannot be null.");
    Preconditions.checkArgument(moduleClass != null, "Dataset module class cannot be null.");
    dataSetModules.put(moduleName, moduleClass.getName());
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    Preconditions.checkArgument(datasetClass != null, "Dataset class cannot be null.");
    dataSetModules.put(datasetClass.getName(), datasetClass.getName());
  }

  @Override
  public void createDataset(String datasetInstanceName, String typeName, DatasetProperties properties) {
    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(typeName != null, "Dataset type name cannot be null.");
    Preconditions.checkArgument(properties != null, "Instance properties name cannot be null.");
    dataSetInstances.put(datasetInstanceName,
                         new DatasetCreationSpec(datasetInstanceName, typeName, properties));
  }

  @Override
  public void createDataset(String datasetInstanceName,
                            Class<? extends Dataset> datasetClass,
                            DatasetProperties properties) {

    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(datasetClass != null, "Dataset class name cannot be null.");
    Preconditions.checkArgument(properties != null, "Instance properties name cannot be null.");
    dataSetInstances.put(datasetInstanceName,
                         new DatasetCreationSpec(datasetInstanceName, datasetClass.getName(), properties));
    dataSetModules.put(datasetClass.getName(), datasetClass.getName());
  }

  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return usePlugin(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Override
  public <T> T usePlugin(final String pluginType, final String pluginName, String pluginId,
                         PluginProperties properties, final PluginSelector selector) {

    Preconditions.checkArgument(!adapterPlugins.containsKey(pluginId),
                                "Plugin of type {}, name {} was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    Map.Entry<PluginInfo, PluginClass> pluginEntry = pluginRepository.findPlugin(adapterConfig.getTemplate(),
                                                                                 pluginType, pluginName, selector);
    if (pluginEntry == null) {
      return null;
    }
    try {
      T instance = pluginInstantiator.newInstance(pluginEntry.getKey(), pluginEntry.getValue(), properties);
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return instance;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName,
                                     String pluginId, PluginProperties properties) {
    return usePluginClass(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties, PluginSelector selector) {

    Preconditions.checkArgument(!adapterPlugins.containsKey(pluginId),
                                "Plugin of type %s, name %s was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    Map.Entry<PluginInfo, PluginClass> pluginEntry = pluginRepository.findPlugin(adapterConfig.getTemplate(),
                                                                                 pluginType, pluginName, selector);
    if (pluginEntry == null) {
      return null;
    }

    // Just verify if all required properties are provided.
    // No type checking is done for now.
    for (PluginPropertyField field : pluginEntry.getValue().getProperties().values()) {
      Preconditions.checkArgument(!field.isRequired() || properties.getProperties().containsKey(field.getName()),
                                  "Required property '%s' missing for plugin of type %s, name %s.",
                                  field.getName(), pluginType, pluginName);
    }

    try {
      Class<T> cls = pluginInstantiator.loadClass(pluginEntry.getKey(), pluginEntry.getValue());
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return cls;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  /**
   * Register the given plugin in this configurer.
   */
  private void registerPlugin(String pluginId, PluginInfo pluginInfo, PluginClass pluginClass,
                              PluginProperties properties) {
    adapterPlugins.put(pluginId, new AdapterPlugin(pluginInfo, pluginClass, properties));
  }

  @Override
  public void setSchedule(Schedule schedule) {
    Preconditions.checkNotNull(schedule, "Schedule cannot be null.");
    Preconditions.checkNotNull(schedule.getName(), "Schedule name cannot be null.");
    Preconditions.checkArgument(!schedule.getName().isEmpty(), "Schedule name cannot be empty.");
    Schedule realSchedule = schedule;
    // prefix with the adapter name so that schedule names are unique
    String realScheduleName = adapterName + "." + schedule.getName();

    if (schedule.getClass().equals(Schedule.class) || schedule instanceof TimeSchedule) {
      realSchedule = Schedules.createTimeSchedule(realScheduleName, schedule.getDescription(),
                                                  schedule.getCronEntry());
    }
    if (schedule instanceof StreamSizeSchedule) {
      StreamSizeSchedule dataSchedule = (StreamSizeSchedule) schedule;
      Preconditions.checkArgument(dataSchedule.getDataTriggerMB() > 0,
                                  "Schedule data trigger must be greater than 0.");
      realSchedule = Schedules.createDataSchedule(realScheduleName, dataSchedule.getDescription(),
                                                  Schedules.Source.STREAM, dataSchedule.getStreamName(),
                                                  dataSchedule.getDataTriggerMB());
    }

    this.schedule = realSchedule;
  }

  @Override
  public void setResources(Resources resources) {
    this.resources = resources;
  }

  @Override
  public void setInstances(int instances) {
    this.instances = instances;
  }

  @Override
  public void addRuntimeArguments(Map<String, String> arguments) {
    runtimeArgs.putAll(arguments);
  }

  @Override
  public void addRuntimeArgument(String key, String value) {
    runtimeArgs.put(key, value);
  }

  public AdapterDefinition createSpecification() {
    AdapterDefinition.Builder builder =
      AdapterDefinition.builder(adapterName, programId)
        .setDescription(adapterConfig.getDescription())
        .setConfig(adapterConfig.getConfig())
        .setDatasets(dataSetInstances)
        .setDatasetModules(dataSetModules)
        .setStreams(streams)
        .setRuntimeArgs(runtimeArgs)
        .setPlugins(adapterPlugins)
        .setInstances(instances)
        .setResources(resources);

    if (programId.getType() == ProgramType.WORKFLOW) {
      String workflowName = Iterables.getFirst(templateSpec.getWorkflows().keySet(), null);
      builder.setScheduleSpec(new ScheduleSpecification(schedule, new ScheduleProgramInfo(
        SchedulableProgramType.WORKFLOW, workflowName), runtimeArgs));
    }

    return builder.build();
  }
}
