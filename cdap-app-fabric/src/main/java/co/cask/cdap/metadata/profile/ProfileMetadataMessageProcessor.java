/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.config.PreferencesTable;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
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
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataKind;
import co.cask.cdap.spi.metadata.MetadataMutation;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
import co.cask.cdap.store.NamespaceTable;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class to process the profile metadata request
 */
public class ProfileMetadataMessageProcessor implements MetadataMessageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileMetadataMessageProcessor.class);
  private static final String PROFILE_METADATA_KEY = "profile";
  private static final Set<ScopedNameOfKind> PROFILE_METADATA_KEY_SET = Collections.singleton(
    new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, PROFILE_METADATA_KEY));
  private static final Set<ProgramType> PROFILE_ALLOWED_PROGRAM_TYPES =
    Arrays.stream(ProgramType.values())
      .filter(SystemArguments::isProfileAllowed)
      .collect(Collectors.toSet());

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())).create();

  private final MetadataStorage metadataStorage;
  private final NamespaceTable namespaceTable;
  private final AppMetadataStore appMetadataStore;
  private final ProgramScheduleStoreDataset scheduleDataset;
  private final PreferencesTable preferencesTable;

  public ProfileMetadataMessageProcessor(MetadataStorage metadataStorage,
                                         StructuredTableContext structuredTableContext) {
    namespaceTable = new NamespaceTable(structuredTableContext);
    appMetadataStore = AppMetadataStore.create(structuredTableContext);
    scheduleDataset = Schedulers.getScheduleStore(structuredTableContext);
    preferencesTable = new PreferencesTable(structuredTableContext);
    this.metadataStorage = metadataStorage;
  }

  @Override
  public void processMessage(MetadataMessage message, StructuredTableContext context)
    throws IOException, ConflictException {

    LOG.trace("Processing message: {}", message);
    EntityId entityId = message.getEntityId();

    switch (message.getType()) {
      case PROFILE_ASSIGNMENT:
      case PROFILE_UNASSIGNMENT:
        validatePreferenceSequenceId(entityId, message);
        updateProfileMetadata(entityId, message);
        break;
      case ENTITY_CREATION:
        validateCreateEntityUpdateTime(entityId, message);
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

  private void validateCreateEntityUpdateTime(EntityId entityId, MetadataMessage message)
    throws IOException, ConflictException {
    // here we expect only APP and SCHEDULE. For apps, no need to validate: that message
    // is emitted after the app is committed to the store. So, only validate for schedules.
    if (entityId.getEntityType() != EntityType.SCHEDULE) {
      return;
    }
    long expectedUpdateTime = GSON.fromJson(message.getRawPayload(), long.class);
    scheduleDataset.ensureUpdateTime((ScheduleId) entityId, expectedUpdateTime);
  }

  private void validatePreferenceSequenceId(EntityId entityId, MetadataMessage message)
    throws IOException, ConflictException {
    long seqId = GSON.fromJson(message.getRawPayload(), long.class);
    preferencesTable.ensureSequence(entityId, seqId);
  }

  private void updateProfileMetadata(EntityId entityId, MetadataMessage message) throws IOException {
    List<MetadataMutation> updates = new ArrayList<>();
    LOG.trace("Updating profile metadata for {}", entityId);
    collectProfileMetadata(entityId, message, updates);
    metadataStorage.batch(updates);
  }

  private void collectProfileMetadata(EntityId entityId, MetadataMessage message,
                                      List<MetadataMutation> updates) throws IOException {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        for (NamespaceMeta meta : namespaceTable.list()) {
          collectProfileMetadata(meta.getNamespaceId(), message, updates);
        }
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        // make sure namespace exists before updating
        if (namespaceTable.get(namespaceId) == null) {
          LOG.debug("Namespace {} is not found, so the profile metadata of programs or schedules in it will not get " +
                      "updated. Ignoring the message {}", namespaceId, message);
          return;
        }
        ProfileId namespaceProfile = getResolvedProfileId(namespaceId);
        List<ApplicationMeta> applicationMetas = appMetadataStore.getAllApplications(namespaceId.getNamespace());
        for (ApplicationMeta meta : applicationMetas) {
          collectAppProfileMetadata(namespaceId.app(meta.getId()), meta.getSpec(), namespaceProfile, updates);
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
        collectAppProfileMetadata(appId, meta.getSpec(), null, updates);
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
          collectProgramProfileMetadata(programId, null, updates);
        }
        break;
      case SCHEDULE:
        ScheduleId scheduleId = (ScheduleId) entityId;
        // make sure the schedule exists before updating
        try {
          ProgramSchedule schedule = scheduleDataset.getSchedule(scheduleId);
          collectScheduleProfileMetadata(schedule, getResolvedProfileId(schedule.getProgramId()), updates);
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
  private void removeProfileMetadata(MetadataMessage message) throws IOException {
    EntityId entity = message.getEntityId();
    List<MetadataMutation> deletes = new ArrayList<>();

    // We only care about application and schedules.
    if (entity.getEntityType().equals(EntityType.APPLICATION)) {
      LOG.trace("Removing profile metadata for {}", entity);
      ApplicationId appId = (ApplicationId) message.getEntityId();
      ApplicationSpecification appSpec = message.getPayload(GSON, ApplicationSpecification.class);
      for (ProgramId programId : getAllProfileAllowedPrograms(appSpec, appId)) {
        addProfileMetadataDelete(programId, deletes);
      }
      for (ScheduleId scheduleId : getSchedulesInApp(appId, appSpec.getProgramSchedules())) {
        addProfileMetadataDelete(scheduleId, deletes);
      }
    } else if (entity.getEntityType().equals(EntityType.SCHEDULE)) {
      addProfileMetadataDelete((NamespacedEntityId) entity, deletes);
    }
    if (!deletes.isEmpty()) {
      metadataStorage.batch(deletes);
    }
  }

  private void collectAppProfileMetadata(ApplicationId applicationId, ApplicationSpecification appSpec,
                                         @Nullable ProfileId namespaceProfile,
                                         List<MetadataMutation> updates) throws IOException {
    ProfileId appProfile = namespaceProfile == null
      ? getResolvedProfileId(applicationId)
      : getProfileId(applicationId).orElse(namespaceProfile);
    for (ProgramId programId : getAllProfileAllowedPrograms(appSpec, applicationId)) {
      collectProgramProfileMetadata(programId, appProfile, updates);
    }
  }

  private void collectProgramProfileMetadata(ProgramId programId, @Nullable ProfileId appProfile,
                                             List<MetadataMutation> updates) throws IOException {
    ProfileId programProfile = appProfile == null
      ? getResolvedProfileId(programId)
      : getProfileId(programId).orElse(appProfile);
    addProfileMetadataUpdate(programId, programProfile, updates);

    for (ProgramSchedule schedule : scheduleDataset.listSchedules(programId)) {
      collectScheduleProfileMetadata(schedule, programProfile, updates);
    }
  }

  private void collectScheduleProfileMetadata(ProgramSchedule schedule, ProfileId programProfile,
                                              List<MetadataMutation> updates) {
    ScheduleId scheduleId = schedule.getScheduleId();
    // if we are able to get profile from preferences or schedule properties, use it
    // otherwise default profile will be used
    Optional<ProfileId> scheduleProfileId =
      SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(), schedule.getProperties());
    programProfile = scheduleProfileId.orElse(programProfile);
    addProfileMetadataUpdate(scheduleId, programProfile, updates);
  }

  private void addProfileMetadataUpdate(NamespacedEntityId entityId, ProfileId profileId,
                                        List<MetadataMutation> updates) {
    LOG.trace("Setting profile metadata for {} to {}", entityId, profileId);
    updates.add(new MetadataMutation.Update(entityId.toMetadataEntity(),
                                            new Metadata(MetadataScope.SYSTEM,
                                                         ImmutableMap.of(PROFILE_METADATA_KEY,
                                                                         profileId.getScopedName()))));
  }

  private void addProfileMetadataDelete(NamespacedEntityId entityId, List<MetadataMutation> deletes) {
    LOG.trace("Deleting profile metadata for {}", entityId);
    deletes.add(new MetadataMutation.Remove(entityId.toMetadataEntity(), PROFILE_METADATA_KEY_SET));
  }

  /**
   * Get the profile id for the provided entity id from the resolved preferences from preference dataset,
   * if no profile is inside, it will return the default profile
   *
   * @param entityId entity id to lookup the profile id
   * @return the profile id which will be used by this entity id, default profile if not find
   */
  // TODO: CDAP-13579 consider preference key starts with [scope].[name].system.profile.name
  private ProfileId getResolvedProfileId(EntityId entityId) throws IOException {
    NamespaceId namespaceId = entityId.getEntityType().equals(EntityType.INSTANCE) ?
      NamespaceId.SYSTEM : ((NamespacedEntityId) entityId).getNamespaceId();
    String profileName = preferencesTable.getResolvedPreference(entityId, SystemArguments.PROFILE_NAME);
    return profileName == null ? ProfileId.NATIVE : ProfileId.fromScopedName(namespaceId, profileName);
  }

  /**
   * Get the profile id for the provided entity id from its own preferences from preference dataset.
   *
   * @param entityId entity id to lookup the profile id
   * @return the profile id configured for this entity id, if any
   */
  private Optional<ProfileId> getProfileId(EntityId entityId) throws IOException {
    NamespaceId namespaceId = entityId.getEntityType().equals(EntityType.INSTANCE) ?
      NamespaceId.SYSTEM : ((NamespacedEntityId) entityId).getNamespaceId();
    String profileName = preferencesTable.getPreferences(entityId).get(SystemArguments.PROFILE_NAME);
    return profileName == null ? Optional.empty() : Optional.of(ProfileId.fromScopedName(namespaceId, profileName));
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
