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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Application application}.
 */
public class AppSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private final ApplicationSpecification appSpec;
  private final ApplicationId appId;
  private final boolean existing;

  public AppSystemMetadataWriter(MetadataStore metadataStore, ApplicationId appId, ApplicationSpecification appSpec) {
    this(metadataStore, appId, appSpec, true);
  }

  public AppSystemMetadataWriter(MetadataStore metadataStore, ApplicationId entityId,
                                 ApplicationSpecification appSpec, boolean existing) {
    super(metadataStore, entityId);
    this.appSpec = appSpec;
    this.appId = entityId;
    this.existing = existing;
  }

  @Override
  protected Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(ENTITY_NAME_KEY, appSpec.getName());
    properties.put(VERSION_KEY, appId.getVersion());
    String description = appSpec.getDescription();
    if (!Strings.isNullOrEmpty(description)) {
      properties.put(DESCRIPTION_KEY, description);
    }
    if (!existing) {
      properties.put(CREATION_TIME_KEY, String.valueOf(System.currentTimeMillis()));
    }
    addPrograms(properties);
    addSchedules(properties);
    // appSpec.getPlugins() returns all instances of all plugins, so there may be duplicates.
    // we only store unique plugins right now
    Set<PluginClass> existingPluginClasses = new HashSet<>();
    for (Plugin plugin : appSpec.getPlugins().values()) {
      if (!existingPluginClasses.contains(plugin.getPluginClass())) {
        addPlugin(plugin.getPluginClass(), null, properties);
        existingPluginClasses.add(plugin.getPluginClass());
      }
    }
    return properties.build();
  }

  @Override
  protected String[] getSystemTagsToAdd() {
    return new String[] {
      appSpec.getArtifactId().getName()
    };
  }

  private void addPrograms(ImmutableMap.Builder<String, String> properties) {
    addPrograms(ProgramType.FLOW, appSpec.getFlows().values(), properties);
    addPrograms(ProgramType.MAPREDUCE, appSpec.getMapReduce().values(), properties);
    addPrograms(ProgramType.SERVICE, appSpec.getServices().values(), properties);
    addPrograms(ProgramType.SPARK, appSpec.getSpark().values(), properties);
    addPrograms(ProgramType.WORKER, appSpec.getWorkers().values(), properties);
    addPrograms(ProgramType.WORKFLOW, appSpec.getWorkflows().values(), properties);
  }

  private void addPrograms(ProgramType programType, Iterable<? extends ProgramSpecification> specs,
                           ImmutableMap.Builder<String, String> properties) {
    for (ProgramSpecification spec : specs) {
      properties.put(programType.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + spec.getName(),
                     spec.getName());
    }
  }

  private void addSchedules(ImmutableMap.Builder<String, String> properties) {
    for (ScheduleCreationSpec creationSpec : appSpec.getProgramSchedules().values()) {
      properties.put("schedule" + MetadataDataset.KEYVALUE_SEPARATOR + creationSpec.getName(),
                     creationSpec.getName() + MetadataDataset.KEYVALUE_SEPARATOR + creationSpec.getDescription());
    }
  }
}
