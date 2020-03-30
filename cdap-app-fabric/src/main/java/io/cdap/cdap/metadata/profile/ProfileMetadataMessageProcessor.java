/*
 * Copyright © 2018-2020 Cask Data, Inc.
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


package io.cdap.cdap.metadata.profile;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleNotFoundException;
import io.cdap.cdap.metadata.ApplicationDetailFetcher;
import io.cdap.cdap.metadata.MetadataMessageProcessor;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.ScheduleFetcher;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
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
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final ApplicationDetailFetcher appDetailFetcher;
  private final PreferencesFetcher preferencesFetcher;
  private final ScheduleFetcher scheduleFetcher;

  public ProfileMetadataMessageProcessor(MetadataStorage metadataStorage,
                                NamespaceQueryAdmin namespaceQueryAdmin,
                                         ApplicationDetailFetcher applicationDetailFetcher,
                                         PreferencesFetcher preferencesFetcher,
                                         ScheduleFetcher scheduleFetcher) {
    this.metadataStorage = metadataStorage;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.appDetailFetcher = applicationDetailFetcher;
    this.preferencesFetcher = preferencesFetcher;
    this.scheduleFetcher = scheduleFetcher;
  }

  /**
   *  Process the given {@link MetadataMessage} and update profile metadata
   *
   * @param message published {@link MetadataMessage}
   * @param context context to initiate {@link StructuredTable}. Not used in this method.
   * @throws IOException if failed to process due to unable to get information or update metadata
   * @throws ConflictException if failed to process due to {@link MetadataMessage} conflicts with the current state
   */
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
        validateEntityCreationTimestamp(entityId, message);
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

  /**
   * Validate the timestamp in the published creation event (i.e. the timestamp of creation event is in the past,
   * in other words: timestamp of published creation event < the timestamp of the most recent update on that entity)
   *
   * @param entityId the id of the entity that was created
   * @param message published message that is received
   * @throws ConflictException if unable to validate or validation failed
   *                           (i.e. published creation timestamp >  most recent update timestamp)
   * @throws IOException if failed to get the timestamp of the most recent update on the entity
   */
  private void validateEntityCreationTimestamp(EntityId entityId, MetadataMessage message)
    throws ConflictException, IOException {
    // Only entity type APPLICATION or SCHEDULE are expected to reach here.
    if (entityId.getEntityType() != EntityType.APPLICATION && entityId.getEntityType() != EntityType.SCHEDULE) {
      LOG.warn("Unexpected entity encountered when validating creation timestamp. Expected types are: {}, {}",
               entityId.toString(), EntityType.APPLICATION, EntityType.SCHEDULE);
    }

    // For APPLICATION, no need to validate, because message is emitted after the app is committed to the store.
    if (entityId.getEntityType() == EntityType.APPLICATION) {
      return;
    }

    // Get the timestamp of the most recent update on the entity (i.e. schedule)
    ScheduleId scheduleId = (ScheduleId) entityId;
    ScheduleDetail schedule = null;
    try {
      schedule = scheduleFetcher.get(scheduleId);
    } catch (NotFoundException e) {
      throw new ConflictException(String.format("Unable to creation time of entity %s due to schedule not found",
                                                entityId.toString()),
                                  e);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, ConflictException.class, IOException.class);
      throw new IOException(String.format("Unable to get schedule %s to validate its update time ",
                                          entityId.toString()),
                            e);
    }

    Long lastUpdateTime = schedule.getLastUpdateTime();
    // This should never happen.
    if (lastUpdateTime == null) {
      throw new ConflictException(String.format("Unexpected null last update timestamp for schedule %s",
                                                entityId.toString()));
    }

    long creationTimeInMessage = GSON.fromJson(message.getRawPayload(), long.class);
    if (creationTimeInMessage > lastUpdateTime.longValue()) {
      throw new ConflictException(String.format("Unexpected: creation time %d > last update time %d for schedule %s",
                                                creationTimeInMessage, lastUpdateTime, scheduleId.toString()));
    }
  }

  /**
   * Validate the sequence id in the published preferences change event (i.e. sequence id in the published message
   * should be less than the latest sequence id for the given entity)
   *
   * @param entityId the id of the entity for the creation event
   * @param message published message received
   * @throws ConflictException if validation failed (i.e. published sequence id > the latest sequence id)
   * @throws IOException if failed to get the latest sequence id for the given entity
   */
  private void validatePreferenceSequenceId(EntityId entityId, MetadataMessage message)
    throws ConflictException, IOException {
    long receivedSeqId = GSON.fromJson(message.getRawPayload(), long.class);
    PreferencesDetail preferences = null;
    try {
      preferences = preferencesFetcher.get(entityId, false);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, ConflictException.class, IOException.class);
      throw new IOException(
        String.format("Unable to get the preferences %s to validate its sequence id", entityId.toString()), e);
    }
    long latestSeqId = preferences.getSeqId();
    if (receivedSeqId > latestSeqId) {
      throw new ConflictException(String.format("Unexpected: received seq id %d > latest seq id %d for preferences %s",
                                                receivedSeqId, latestSeqId, entityId.toString()));
    }
  }

  private void updateProfileMetadata(EntityId entityId, MetadataMessage message) throws IOException {
    List<MetadataMutation> updates = new ArrayList<>();
    LOG.trace("Updating profile metadata for {}", entityId);
    collectProfileMetadata(entityId, message, updates);
    metadataStorage.batch(updates, MutationOptions.DEFAULT);
  }

  private void collectProfileMetadata(EntityId entityId, MetadataMessage message,
                                      List<MetadataMutation> updates) throws IOException {
    switch (entityId.getEntityType()) {
      case INSTANCE: {
        buildUpdatesForInstance(message, updates);
        break;
      }
      case NAMESPACE: {
        buildUpdatesForNamespace((NamespaceId) entityId, message, updates);
        break;
      }
      case APPLICATION: {
        buildUpdatesForApplication((ApplicationId) entityId, message, updates);
        break;
      }
      case PROGRAM: {
        buildUpdatesForProgram((ProgramId) entityId, message, updates);
        break;
      }
      case SCHEDULE: {
        buildUpdatesForSchedule((ScheduleId) entityId, message, updates);
        break;
      }
      default:
        // this should not happen
        LOG.warn("Type of the entity id {} cannot be used to update profile metadata. " +
                   "Ignoring the message {}", entityId, message);
    }
  }

  private void buildUpdatesForInstance(MetadataMessage message,
                                              List<MetadataMutation> updates) throws IOException {
    List<NamespaceMeta> namespaceMetaList = Collections.EMPTY_LIST;
    try {
      namespaceMetaList = namespaceQueryAdmin.list();
    } catch (Exception e) {
      throw new IOException("Failed to list namespaces", e);
    }
    for (NamespaceMeta meta : namespaceMetaList) {
      buildUpdatesForNamespace(meta.getNamespaceId(), message, updates);
    }
  }

  private void buildUpdatesForNamespace(NamespaceId namespaceId, MetadataMessage message,
                                              List<MetadataMutation> updates) throws IOException {
    // make sure namespace exists before updating
    try {
      namespaceQueryAdmin.get(namespaceId);
    } catch (NamespaceNotFoundException e) {
      LOG.debug("Namespace {} is not found, so the profile metadata of programs or schedules in it will not get " +
                  "updated. Ignoring the message {}", namespaceId, message);
      return;
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(String.format("Failed to check if namespace %s eixsts", namespaceId.toString()),
                            e);
    }

    List<ApplicationDetail> appDetailList = Collections.emptyList();
    try {
      appDetailList = appDetailFetcher.list(namespaceId.getNamespace());
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(String.format("Failed to get details of applications in namespace %s",
                                          namespaceId.toString()),
                            e);
    }

    ProfileId namespaceProfileId = getAssignedProfileId(namespaceId, true, ProfileId.NATIVE);
    // TODO: switch to batch calls
    for (ApplicationDetail appDetail : appDetailList) {
      collectAppProfileMetadata(namespaceId.app(appDetail.getName()), appDetail, namespaceProfileId, updates);
    }
  }

  private void buildUpdatesForApplication(ApplicationId appId, MetadataMessage message,
                                                List<MetadataMutation> updates) throws IOException {
    // make sure app exists before updating
    ApplicationDetail appDetail;
    try {
      appDetail = appDetailFetcher.get(appId);
    } catch (NotFoundException e) {
      LOG.debug("Fail to get metadata of application {}, so the profile metadata of its programs/schedules " +
                  "will not get updated. Ignoring the message {}: {}", appId, message, e);
      return;
    } catch (Exception e) {
      throw new IOException(String.format("Failed to get application detail for application %s", appId.toString()),
                            e);
    }
    collectAppProfileMetadata(appId, appDetail, null, updates);
  }


  private void buildUpdatesForProgram(ProgramId programId, MetadataMessage message,
                                            List<MetadataMutation> updates) throws IOException {
    ApplicationId appId = programId.getParent();
    // make sure the app of the program exists before updating
    try {
      appDetailFetcher.get(appId);
    } catch (NotFoundException e) {
      LOG.debug("Application {} is not found, so the profile metadata of program {} will not get updated. " +
                  "Ignoring the message {}", appId, programId, message);
      return;
    }
    if (SystemArguments.isProfileAllowed(programId.getType())) {
      collectProgramProfileMetadata(programId, null, updates);
    }
  }

  private void buildUpdatesForSchedule(ScheduleId scheduleId, MetadataMessage message,
                                             List<MetadataMutation> updates) throws IOException {
    // Make sure the schedule exists before updating
    ScheduleDetail schedule = null;
    try {
      schedule = scheduleFetcher.get(scheduleId);
    } catch (ScheduleNotFoundException e) {
      LOG.debug("Schedule {} is not found, so its profile metadata will not get updated. " +
                  "Ignoring the message {}", scheduleId, message);
      return;
    } catch (Exception e) {
      throw new IOException(String.format("Unable to get the detail of schedule %s",
                                          scheduleId.toString()),
                            e);
    }
    // Get the profile id assigned to the program that this schedule is for.
    // This is used as default profile id for the schedule if there is no profile id set on the schedule.
    ProgramType programType = ProgramType.valueOfSchedulableType(schedule.getProgram().getProgramType());
    ProgramId programId = new ProgramId(schedule.getNamespace(), schedule.getApplication(), programType,
                                        schedule.getProgram().getProgramName());
    ProfileId programProfileId = getAssignedProfileId(programId, true, ProfileId.NATIVE);

    // Build profile metadata updates for ScheduleId -> ProfileId mapping
    collectScheduleProfileMetadata(schedule, programProfileId, updates);
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
      for (ProgramId programId : getProfileAllowedPrograms(appSpec, appId)) {
        addProfileMetadataDelete(programId, deletes);
      }
      for (ScheduleId scheduleId : getScheduleIdsFromNames(appId, appSpec.getProgramSchedules().keySet())) {
        addProfileMetadataDelete(scheduleId, deletes);
      }
    } else if (entity.getEntityType().equals(EntityType.SCHEDULE)) {
      addProfileMetadataDelete((NamespacedEntityId) entity, deletes);
    }
    if (!deletes.isEmpty()) {
      metadataStorage.batch(deletes, MutationOptions.DEFAULT);
    }
  }

  /**
   * Find the profile ids assigned to all schedules and programs in the given application and add
   * metadata update operations for these assignments to the list of mutations that will be applied on
   * {@link MetadataStorage}
   *
   * @param applicationId the id of the application for which to find all profiles assigned to schedules of its programs
   * @param appDetail details of the given application
   * @param defaultProfileId default profile id to use if no profile found for a schedule
   * @param updates a list of mutations to be applied on {@link MetadataStorage}
   * @throws IOException if failed to get
   */
  private void collectAppProfileMetadata(ApplicationId applicationId, ApplicationDetail appDetail,
                                         @Nullable ProfileId defaultProfileId,
                                         List<MetadataMutation> updates) throws IOException {
    ProfileId appProfile = defaultProfileId == null
      ? getAssignedProfileId(applicationId, true, ProfileId.NATIVE)
      : getAssignedProfileId(applicationId, false, defaultProfileId);
    for (ProgramId programId : getProfileAllowedPrograms(appDetail, applicationId)) {
      collectProgramProfileMetadata(programId, appProfile, updates);
    }
  }

  /**
   * Find the profile ids assigned to all schedules set on the given program and add metadata update operations
   * for these assignments to the list of mutations that will be applied on {@link MetadataStorage}
   *
   * @param programId the id of the program for which to find all profiles assigned to schedules configured for it
   * @param defaultProfileId default profile id to use when no profile assigned to a schedule
   * @param updates a list of mutations to be applied on {@link MetadataStorage}
   * @throws IOException if failed to list all schedules configured for the given program
   */
  private void collectProgramProfileMetadata(ProgramId programId, @Nullable ProfileId defaultProfileId,
                                             List<MetadataMutation> updates) throws IOException {
    ProfileId programProfileId = defaultProfileId == null
      ? getAssignedProfileId(programId, true, ProfileId.NATIVE)
      : getAssignedProfileId(programId, false, defaultProfileId);
    addProfileMetadataUpdate(programId, programProfileId, updates);

    List<ScheduleDetail> scheduleList = Collections.emptyList();
    try {
      scheduleList = scheduleFetcher.list(programId);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new IOException(String.format("Failed to list scheudles for program id %s", programId.toString()),
                            e.getCause());
    }
    scheduleList.forEach(schedule -> collectScheduleProfileMetadata(schedule, programProfileId, updates));
  }

  /**
   * Find the profile id assigned to the schedule from the given {@link ScheduleDetail} and add an metadata
   * update operation for this assignment to the list of mutations that will be applied to metadata storage.
   *
   * @param schedule details of a schedule containing profile id assigned to this schedule, if any, in its property map
   * @param defaultProfileId default profile id to use if no profile id found in schedule detail
   * @param updates a list of mutations to be applied on {@link MetadataStorage}
   */
  private void collectScheduleProfileMetadata(ScheduleDetail schedule, ProfileId defaultProfileId,
                                              List<MetadataMutation> updates) {
    ScheduleId scheduleId = schedule.toScheduleId();
    // If we are able to get profile from preferences or schedule properties, use it
    // otherwise default profile will be used
    ProfileId profileId = SystemArguments.getProfileIdFromArgs(scheduleId.getNamespaceId(),
                                                               schedule.getProperties()).orElse(defaultProfileId);
    addProfileMetadataUpdate(scheduleId, profileId, updates);
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
   * Get the profile id assigned to the given entity id from the preferences associated with it
   *
   * @param entityId entity id for which to look up assigned profile id
   * @param resolved whether to use resolve preferences for looking up assigned profile id
   * @param defaultId default profile id to use if no assigned profile id for the entity is found
   * @return profile id assigned to the entity
   * @throws IOException
   */
  private ProfileId getAssignedProfileId(EntityId entityId, boolean resolved, ProfileId defaultId) throws IOException {
    NamespaceId namespaceId = entityId.getEntityType().equals(EntityType.INSTANCE) ?
      NamespaceId.SYSTEM : ((NamespacedEntityId) entityId).getNamespaceId();
    PreferencesDetail preferences = null;
    try {
      preferences = preferencesFetcher.get(entityId, resolved);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new IOException(String.format("Failed to get preferences for entity %s", entityId.toString()),
                            e.getCause());
    }
    String profileName = preferences.getProperties().get(SystemArguments.PROFILE_NAME);
    return profileName == null ? defaultId : ProfileId.fromScopedName(namespaceId, profileName);
  }

  /**
   * Get the schedule ids for the given list of schedule names in the supplied appliation.
   */
  private Set<ScheduleId> getScheduleIdsFromNames(ApplicationId appId,
                                                  Set<String> scheduleSet) {
    return scheduleSet.stream().map(appId::schedule).collect(Collectors.toSet());
  }

  /**
   * Gets all programIds in the {code appSpec} which are allowed to use profiles.
   */
  private Set<ProgramId> getProfileAllowedPrograms(ApplicationSpecification appSpec, ApplicationId appId) {
    Function<ProgramType, Set<String>> getProgramsByType =
      programType -> appSpec.getProgramsByType(programType.getApiProgramType());
    return getProfileAllowedProgramsInternal(getProgramsByType, appId);
  }

  /**
   * Gets all programIds in the {@code appDetail} which are allowed to use profiles.
   */
  private Set<ProgramId> getProfileAllowedPrograms(ApplicationDetail appDetail, ApplicationId appId) {
    Function<ProgramType, Set<String>> getProgramsByType = programType -> {
      Set<String> programs = new HashSet<>();
      for (ProgramRecord programRecord : appDetail.getPrograms()) {
        if (programRecord.getType() == programType) {
          programs.add(programRecord.getName());
        }
      }
      return programs;
    };
    return getProfileAllowedProgramsInternal(getProgramsByType, appId);
  }

  private Set<ProgramId> getProfileAllowedProgramsInternal(Function<ProgramType, Set<String>> getProgramByType,
                                                           ApplicationId appId) {
    return PROFILE_ALLOWED_PROGRAM_TYPES.stream()
      .flatMap(type -> getProgramByType.apply(type).stream().map(name -> appId.program(type, name)))
      .collect(Collectors.toSet());
  }
}
