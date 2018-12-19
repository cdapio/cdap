/*
 * Copyright © 2018 Cask Data, Inc.
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


package co.cask.cdap.metadata.profile;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.config.PreferencesDataset;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.ApplicationMeta;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.metadata.MetadataMessageProcessor;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.store.NamespaceMDS;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to process the profile metadata request
 */
public class ProfileMetadataMessageProcessor implements MetadataMessageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileMetadataMessageProcessor.class);
  private static final String PROFILE_METADATA_KEY = "profile";
  private static final Set<ProgramType> PROFILE_ALLOWED_PROGRAM_TYPES =
    Arrays.stream(ProgramType.values())
      .filter(SystemArguments::isProfileAllowed)
      .collect(Collectors.toSet());

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())).create();

  private final NamespaceMDS namespaceMDS;
  private final AppMetadataStore appMetadataStore;
  private final ProgramScheduleStoreDataset scheduleDataset;
  private final PreferencesDataset preferencesDataset;
  private final MetadataDataset metadataDataset;

  public ProfileMetadataMessageProcessor(CConfiguration cConf, DatasetContext datasetContext,
                                         DatasetFramework datasetFramework) {
    namespaceMDS = NamespaceMDS.getNamespaceMDS(datasetContext, datasetFramework);
    appMetadataStore = AppMetadataStore.create(cConf, datasetContext, datasetFramework);
    scheduleDataset = Schedulers.getScheduleStore(datasetContext, datasetFramework);
    preferencesDataset = PreferencesDataset.get(datasetContext, datasetFramework);
    metadataDataset = DefaultMetadataStore.getMetadataDataset(datasetContext, datasetFramework, MetadataScope.SYSTEM);
  }

  @Override
  public void processMessage(MetadataMessage message) {
    LOG.trace("Processing message: {}", message);

    EntityId entityId = message.getEntityId();

    switch (message.getType()) {
      case PROFILE_ASSIGNMENT:
      case PROFILE_UNASSIGNMENT:
      case ENTITY_CREATION:
        updateProfileMetadata(entityId, message);
        break;
      case ENTITY_DELETION:
        removeProfileMetadata(message);
        break;

      default:
        // This shouldn't happen
        LOG.warn("Unknown message type for profile metadata update. Ignoring the message {}", message);
    }
  }

  private void updateProfileMetadata(EntityId entityId, MetadataMessage message) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        for (NamespaceMeta meta : namespaceMDS.list()) {
          updateProfileMetadata(meta.getNamespaceId(), message);
        }
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        // make sure namespace exists before updating
        if (namespaceMDS.get(namespaceId) == null) {
          LOG.debug("Namespace {} is not found, so the profile metadata of programs or schedules in it will not get " +
                      "updated. Ignoring the message {}", namespaceId, message);
          return;
        }
        LOG.trace("Updating profile metadata for {}", entityId);
        for (ApplicationMeta meta : appMetadataStore.getAllApplications(namespaceId.getNamespace())) {
          updateAppProfileMetadata(namespaceId.app(meta.getId()), meta.getSpec());
        }
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        // make sure app exists before updating
        ApplicationMeta meta = appMetadataStore.getApplication(appId);
        if (meta == null) {
          LOG.debug("Application {} is not found, so the profile metadata of its programs/schedules will not get " +
                      "updated. Ignoring the message {}", appId, message);
          return;
        }
        updateAppProfileMetadata(appId, meta.getSpec());
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        // make sure the app of the program exists before updating
        meta = appMetadataStore.getApplication(programId.getParent());
        if (meta == null) {
          LOG.debug("Application {} is not found, so the profile metadata of program {} will not get updated. " +
                      "Ignoring the message {}", programId.getParent(), programId, message);
          return;
        }

        if (SystemArguments.isProfileAllowed(programId.getType())) {
          updateProgramProfileMetadata(programId);
        }
        break;
      case SCHEDULE:
        ScheduleId scheduleId = (ScheduleId) entityId;
        // make sure the schedule exists before updating
        try {
          ProgramSchedule schedule = scheduleDataset.getSchedule(scheduleId);
          updateScheduleProfileMetadata(schedule, getResolvedProfileId(schedule.getProgramId()));
        } catch (NotFoundException e) {
          LOG.debug("Schedule {} is not found, so its profile metadata will not get updated. " +
                      "Ignoring the message {}", scheduleId, message);
          return;
        }

        break;
      default:
        // this should not happen
        LOG.warn("Type of the entity id {} cannot be used to update profile metadata. " +
                   "Ignoring the message {}", entityId, message);
    }
  }

  /**
   * Remove the profile metadata according to the message, currently only meant for application and schedule.
   */
  private void removeProfileMetadata(MetadataMessage message) {
    EntityId entity = message.getEntityId();

    // We only care about application and schedules.
    if (entity.getEntityType().equals(EntityType.APPLICATION)) {
      ApplicationId appId = (ApplicationId) message.getEntityId();
      ApplicationSpecification appSpec = message.getPayload(GSON, ApplicationSpecification.class);
      for (ProgramId programId : getAllProfileAllowedPrograms(appSpec, appId)) {
        metadataDataset.removeProperties(programId.toMetadataEntity(), Collections.singleton(PROFILE_METADATA_KEY));
      }
      for (ScheduleId scheduleId : getSchedulesInApp(appId, appSpec.getProgramSchedules())) {
        metadataDataset.removeProperties(scheduleId.toMetadataEntity(), Collections.singleton(PROFILE_METADATA_KEY));
      }
    }

    if (entity.getEntityType().equals(EntityType.SCHEDULE)) {
      metadataDataset.removeProperties(message.getEntityId().toMetadataEntity(),
                                       Collections.singleton(PROFILE_METADATA_KEY));
    }
  }

  private void updateAppProfileMetadata(ApplicationId applicationId, ApplicationSpecification appSpec) {
    LOG.trace("Updating profile metadata for {}", applicationId);
    for (ProgramId programId : getAllProfileAllowedPrograms(appSpec, applicationId)) {
      updateProgramProfileMetadata(programId);
    }
  }

  private void updateProgramProfileMetadata(ProgramId programId) {
    LOG.trace("Updating profile metadata for {}", programId);
    ProfileId profileId = getResolvedProfileId(programId);
    setProfileMetadata(programId, profileId);

    for (ProgramSchedule schedule : scheduleDataset.listSchedules(programId)) {
      updateScheduleProfileMetadata(schedule, profileId);
    }
  }

  private void updateScheduleProfileMetadata(ProgramSchedule schedule, ProfileId profileId) {
    ScheduleId scheduleId = schedule.getScheduleId();
    LOG.trace("Updating profile metadata for {}", scheduleId);
    // if we are able to get profile from preferences or schedule properties, use it
    // otherwise default profile will be used
    Optional<ProfileId> scheduleProfileId =
      SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(), schedule.getProperties());
    profileId = scheduleProfileId.orElse(profileId);
    setProfileMetadata(scheduleId, profileId);
  }

  private void setProfileMetadata(NamespacedEntityId entityId, ProfileId profileId) {
    LOG.trace("Setting profile metadata for {} to {}", entityId, profileId);
    metadataDataset.setProperty(entityId.toMetadataEntity(), PROFILE_METADATA_KEY, profileId.getScopedName());
  }

  /**
   * Get the profile id for the provided entity id from the resolved preferences from preference dataset,
   * if no profile is inside, it will return the default profile
   *
   * @param entityId entity id to lookup the profile id
   * @return the profile id which will be used by this entity id, default profile if not find
   */
  // TODO: CDAP-13579 consider preference key starts with [scope].[name].system.profile.name
  private ProfileId getResolvedProfileId(EntityId entityId) {
    NamespaceId namespaceId = entityId.getEntityType().equals(EntityType.INSTANCE) ?
      NamespaceId.SYSTEM : ((NamespacedEntityId) entityId).getNamespaceId();
    return SystemArguments.getProfileIdFromArgs(
      namespaceId, preferencesDataset.getResolvedPreferences(entityId)).orElse(ProfileId.NATIVE);
  }

  /**
   * Get the schedule id from the schedule spec
   */
  private Set<ScheduleId> getSchedulesInApp(ApplicationId appId,
                                            Map<String, ? extends ScheduleCreationSpec> scheduleSpecs) {
    Set<ScheduleId> result = new HashSet<>();

    for (String programName : scheduleSpecs.keySet()) {
      result.add(appId.schedule(programName));
    }
    return result;
  }

  /**
   * Gets all programIds which are defined in the appSpec which are allowed to use profiles.
   */
  private Set<ProgramId> getAllProfileAllowedPrograms(ApplicationSpecification appSpec, ApplicationId appId) {
    Set<ProgramId> programIds = new HashSet<>();
    for (ProgramType programType : PROFILE_ALLOWED_PROGRAM_TYPES) {
      for (String name : appSpec.getProgramsByType(programType.getApiProgramType())) {
        programIds.add(appId.program(programType, name));
      }
    }
    return programIds;
  }
}
