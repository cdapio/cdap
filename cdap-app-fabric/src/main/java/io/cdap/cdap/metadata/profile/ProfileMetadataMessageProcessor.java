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
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NamespaceNotFoundException;
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
                                         ApplicationDetailFetcher appDetailFetcher,
                                         PreferencesFetcher preferencesFetcher,
                                         ScheduleFetcher scheduleFetcher) {
    this.metadataStorage = metadataStorage;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.appDetailFetcher = appDetailFetcher;
    this.preferencesFetcher = preferencesFetcher;
    this.scheduleFetcher = scheduleFetcher;
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

  /**
   * Validate the timestamp in the published creation event is valid (i.e. < the timestamp of the most recent update
   * on the target entity).
   *
   * @param entityId the id of the entity that was created
   * @param message published message received
   * @throws ConflictException if validation failed (i.e. published update time > the most recent update time)
   */
  private void validateCreateEntityUpdateTime(EntityId entityId, MetadataMessage message)
    throws ConflictException {
    // here we expect only APP and SCHEDULE. For apps, no need to validate: that message
    // is emitted after the app is committed to the store. So, only validate for schedules.
    if (entityId.getEntityType() != EntityType.SCHEDULE) {
      return;
    }
    long creationTime = GSON.fromJson(message.getRawPayload(), long.class);
    ScheduleId scheduleId = (ScheduleId) entityId;
    ScheduleDetail schedule = null;
    try {
      schedule = scheduleFetcher.get(scheduleId);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new ConflictException(
        String.format("Unable to get the schedule %s to validate its update time ", entityId.toString()), e);
    }
    Long lastUpdateTime = schedule.getLastUpdateTime();
    if (creationTime > lastUpdateTime) {
      throw new ConflictException(String.format("Unexpected: creation time %d > last update time %d for schedule %s",
                                                creationTime, lastUpdateTime, scheduleId.toString()));
    }
  }

  /**
   * Validate the sequence id in the published preferences change event is valid
   * (i.e. sequence id in the published message < the latest sequence id)
   *
   * @param entityId the id of the entity for the creation event
   * @param message published message received
   * @throws ConflictException if validation failed (i.e. published sequence id > the latest sequence id)
   */
  private void validatePreferenceSequenceId(EntityId entityId, MetadataMessage message) throws ConflictException {
    long receivedSeqId = GSON.fromJson(message.getRawPayload(), long.class);
    PreferencesDetail preferences;
    try {
      preferences = preferencesFetcher.get(entityId, false);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new ConflictException(
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
    NamespaceId namespaceId = null;
    ApplicationId appId = null;
    ProgramId programId = null;
    ScheduleId scheduleId = null;

    // TODO: extract the logic in each switch case to a method
    switch (entityId.getEntityType()) {
      case INSTANCE:
        List<NamespaceMeta> metaList = Collections.EMPTY_LIST;
        try {
          metaList = namespaceQueryAdmin.list();
        } catch (Exception e) {
          Throwables.propagateIfPossible(e);
          throw new IOException(e);
        }
        for (NamespaceMeta meta : metaList) {
          collectProfileMetadata(meta.getNamespaceId(), message, updates);
        }
        break;
      case NAMESPACE:
        namespaceId = (NamespaceId) entityId;
        // make sure namespace exists before updating
        try {
          namespaceQueryAdmin.get(namespaceId);
        } catch (NamespaceNotFoundException e) {
          LOG.debug("Namespace {} is not found, so the profile metadata of programs or schedules in it will not get " +
                      "updated. Ignoring the message {}", namespaceId, message);
          return;
        } catch (Exception e) {
          throw new IOException(e);
        }
        ProfileId namespaceProfile = getAssignedProfileId(namespaceId, true, ProfileId.NATIVE);
        List<ApplicationDetail> appDetailList = Collections.emptyList();
        try {
          appDetailList = appDetailFetcher.list(namespaceId.getNamespace());
        } catch (NamespaceNotFoundException e) {
          throw new IOException(String.format("Namespace %s not found, may have just been deleted"), e);
        }
        for (ApplicationDetail appDetail : appDetailList) {
          System.out.println("wyzhang " + appDetail.toString());
          collectAppProfileMetadata(namespaceId.app(appDetail.getName()), appDetail, namespaceProfile, updates);
        }
        break;
      case APPLICATION:
        appId = (ApplicationId) entityId;
        // make sure app exists before updating
        ApplicationDetail appDetail;
        try {
          appDetail = appDetailFetcher.get(appId);
        } catch (ApplicationNotFoundException e) {
          LOG.debug("Fail to get metadata of application {}, so the profile metadata of its programs/schedules will not get " +
                      "updated. Ignoring the message {}: {}", appId, message, e);
          return;
        } catch (Exception e) {
          throw new IOException(String.format("Failed to get application detail for application %s", appId.toString()),
                                e);
        }
        collectAppProfileMetadata(appId, appDetail, null, updates);
        break;
      case PROGRAM:
        programId = (ProgramId) entityId;
        appId = programId.getParent();
        // make sure the app of the program exists before updating
        try {
          appDetailFetcher.get(appId);
        } catch (ApplicationNotFoundException e) {
          LOG.debug("Application {} is not found, so the profile metadata of program {} will not get updated. " +
                      "Ignoring the message {}", appId, programId, message);
          return;
        } catch (Exception e) {
          throw new IOException(
            String.format("Failed to get application detail for application %s", appId.toString()), e);
        }
        if (SystemArguments.isProfileAllowed(programId.getType())) {
          collectProgramProfileMetadata(programId, null, updates);
        }
        break;
      case SCHEDULE:
        scheduleId = (ScheduleId) entityId;
        // make sure the schedule exists before updating
        ScheduleDetail schedule = null;
        try {
          schedule = scheduleFetcher.get(scheduleId);
        } catch (ScheduleNotFoundException e) {
          LOG.debug("Schedule {} is not found, so its profile metadata will not get updated. " +
                      "Ignoring the message {}", scheduleId, message);
          return;
        } catch (Exception e) {
          throw new IOException(e);
        }
        ProgramType programType = ProgramType.valueOfSchedulableType(schedule.getProgram().getProgramType());
        programId = new ProgramId(schedule.getNamespace(), schedule.getApplication(), programType,
                                  schedule.getProgram().getProgramName());
        collectScheduleProfileMetadata(schedule, getAssignedProfileId(programId, true, ProfileId.NATIVE), updates);
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
      throw new IOException(String.format("Failed to list scheudles for program id %s", programId.toString()), e.getCause());
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
    Set<ScheduleId> result = new HashSet<>();
    scheduleSet.forEach(schedule -> result.add(appId.schedule(schedule)));
    return result;
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
    Set<ProgramId> programIds = new HashSet<>();
    for (ProgramType programType : PROFILE_ALLOWED_PROGRAM_TYPES) {
      for (String name : getProgramByType.apply(programType)) {
        programIds.add(appId.program(programType, name));
      }
    }
    return programIds;
  }
}
