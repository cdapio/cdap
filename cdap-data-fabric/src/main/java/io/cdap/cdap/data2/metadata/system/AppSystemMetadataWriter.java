/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.system;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.spi.metadata.MetadataConstants;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Application application}.
 */
public class AppSystemMetadataWriter extends AbstractSystemMetadataWriter {

  public static final String CAPABILITY_TAG = "capability";
  public static final String CAPABILITY_DELIMITER = ",";
  private final ApplicationSpecification appSpec;
  private final ApplicationId appId;
  private final ApplicationClass applicationClass;
  private final String creationTime;

  public AppSystemMetadataWriter(MetadataServiceClient metadataServiceClient, ApplicationId entityId,
                                 ApplicationSpecification appSpec, ApplicationClass applicationClass,
                                 String creationTime) {
    super(metadataServiceClient, entityId);
    this.appSpec = appSpec;
    this.appId = entityId;
    this.applicationClass = applicationClass;
    this.creationTime = creationTime;
  }

  @Override
  public Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(MetadataConstants.ENTITY_NAME_KEY, appSpec.getName());
    properties.put(VERSION_KEY, appId.getVersion());
    String description = appSpec.getDescription();
    if (!Strings.isNullOrEmpty(description)) {
      properties.put(MetadataConstants.DESCRIPTION_KEY, description);
    }
    properties.put(MetadataConstants.CREATION_TIME_KEY, creationTime);
    addPrograms(properties);
    addSchedules(properties);
    // appSpec.getPlugins() returns all instances of all plugins, so there may be duplicates.
    // we only store unique plugins right now
    Set<PluginClass> existingPluginClasses = new HashSet<>();
    for (Plugin plugin : appSpec.getPlugins().values()) {
      if (!existingPluginClasses.contains(plugin.getPluginClass())) {
        SystemMetadataProvider.addPlugin(plugin.getPluginClass(), null, properties);
        existingPluginClasses.add(plugin.getPluginClass());
      }
    }
    addCapabilities(appSpec, applicationClass, properties);
    return properties.build();
  }

  private void addCapabilities(ApplicationSpecification appSpec,
                               ApplicationClass applicationClass,
                               ImmutableMap.Builder<String, String> properties) {
    //add from all plugins
    Set<String> capabilitiesSet = appSpec.getPlugins().values().stream()
      .map(Plugin::getPluginClass)
      .map(PluginClass::getRequirements)
      .map(Requirements::getCapabilities)
      .flatMap(Set::stream)
      .collect(Collectors.toSet());
    //add from application
    capabilitiesSet.addAll(applicationClass.getRequirements().getCapabilities());
    if (capabilitiesSet.isEmpty()) {
      return;
    }
    properties.put(CAPABILITY_TAG, String.join(CAPABILITY_DELIMITER, capabilitiesSet));
  }

  @Override
  public Set<String> getSystemTagsToAdd() {
    return Collections.singleton(appSpec.getArtifactId().getName());
  }

  private void addPrograms(ImmutableMap.Builder<String, String> properties) {
    addPrograms(ProgramType.MAPREDUCE, appSpec.getMapReduce().values(), properties);
    addPrograms(ProgramType.SERVICE, appSpec.getServices().values(), properties);
    addPrograms(ProgramType.SPARK, appSpec.getSpark().values(), properties);
    addPrograms(ProgramType.WORKER, appSpec.getWorkers().values(), properties);
    addPrograms(ProgramType.WORKFLOW, appSpec.getWorkflows().values(), properties);
  }

  private void addPrograms(ProgramType programType, Iterable<? extends ProgramSpecification> specs,
                           ImmutableMap.Builder<String, String> properties) {
    for (ProgramSpecification spec : specs) {
      properties.put(programType.getPrettyName() + MetadataConstants.KEYVALUE_SEPARATOR + spec.getName(),
                     spec.getName());
    }
  }

  private void addSchedules(ImmutableMap.Builder<String, String> properties) {
    for (ScheduleCreationSpec creationSpec : appSpec.getProgramSchedules().values()) {
      properties.put("schedule" + MetadataConstants.KEYVALUE_SEPARATOR + creationSpec.getName(),
                     creationSpec.getName() + MetadataConstants.KEYVALUE_SEPARATOR + creationSpec.getDescription());
    }
  }
}
