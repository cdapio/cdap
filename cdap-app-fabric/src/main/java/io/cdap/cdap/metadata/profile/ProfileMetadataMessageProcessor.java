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


package io.cdap.cdap.metadata.profile;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.metadata.MetadataMessageProcessor;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.PreferencesMetadata;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduleMetadata;
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
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
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
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final RemoteClient remoteClient;

  public ProfileMetadataMessageProcessor(MetadataStorage metadataStorage,
                                         NamespaceQueryAdmin namespaceQueryAdmin,
                                         DiscoveryServiceClient discoveryServiceClient) {
    this.metadataStorage = metadataStorage;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         String.format("%s", Constants.Gateway.API_VERSION_3));
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
    long creationTime = GSON.fromJson(message.getRawPayload(), long.class);

    ScheduleId scheduleId = (ScheduleId) entityId;
    ScheduleMetadata metadata = null;
    try {
      metadata = getScheduleMetadata(scheduleId);
    } catch (Exception e) {
      throw new IOException(String.format("Failed to get metadata for schedule %s", scheduleId.toString()), e);
    }
    Long lastUpdateTime = metadata.getLastUpdateTime();
    if (creationTime > lastUpdateTime) {
      throw new ConflictException(String.format(
        "Unexpected: creation time %d > last update time %d for schedule %s",
        creationTime, lastUpdateTime, scheduleId.toString()));
    }
  }

  private void validatePreferenceSequenceId(EntityId entityId, MetadataMessage message)
    throws IOException, ConflictException {
    long seqId = GSON.fromJson(message.getRawPayload(), long.class);
    PreferencesMetadata metadata = getPreferencesMetadata(entityId);
    long latestSeqId = metadata.getSeqId();
    if (seqId > latestSeqId) {
      throw new ConflictException(String.format(
        "Unexpected: seq id %d > lastest seq id %d for preferences %s",
        seqId, latestSeqId, entityId.toString()));
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
      case INSTANCE:
        List<NamespaceMeta> namespaceMetaList;
        try {
          namespaceMetaList = namespaceQueryAdmin.list();
        } catch (Exception e) {
          throw new IOException("Failed to list namespaces", e);
        }
        for (NamespaceMeta meta : namespaceMetaList) {
          collectProfileMetadata(meta.getNamespaceId(), message, updates);
        }
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        // make sure namespace exists before updating
        try {
          if (!namespaceQueryAdmin.exists(namespaceId)) {
            LOG.debug("Namespace {} is not found, so the profile metadata of programs or schedules in it will not get " +
                        "updated. Ignoring the message {}", namespaceId, message);
            return;
          }
        } catch (Exception e) {
          throw new IOException(String.format("Failed to check if namespace %s exists", namespaceId.toString()), e);
        }
        ProfileId namespaceProfile = getResolvedProfileId(namespaceId);
        List<ApplicationMeta> applicationMetaList;
        applicationMetaList = getAllApplicationMetadata(namespaceId.getNamespace());
        for (ApplicationMeta meta : applicationMetaList) {
          collectAppProfileMetadata(namespaceId.app(meta.getId()), meta.getSpec(), namespaceProfile, updates);
        }
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        // make sure app exists before updating
        ApplicationMeta meta;
        try {
          meta = getApplicationMeta(appId);
        } catch (NotFoundException e) {
          LOG.debug("Fail to get metadata of application {}, so the profile metadata of its programs/schedules will not get " +
                      "updated. Ignoring the message {}: {}", appId, message, e);
          return;
        }
        collectAppProfileMetadata(appId, meta.getSpec(), null, updates);
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        // make sure the app of the program exists before updating
        try {
          getApplicationMeta(programId.getParent());
        } catch (NotFoundException e) {
          LOG.debug("Failed to get metadata of application {}, so the profile metadata of program {} will not get updated. " +
                      "Ignoring the message {}: {}", programId.getParent(), programId, message, e);
          return;
        }
        if (SystemArguments.isProfileAllowed(programId.getType())) {
          collectProgramProfileMetadata(programId, null, updates);
        }
        break;
      case SCHEDULE:
        ScheduleId scheduleId = (ScheduleId) entityId;
        ScheduleDetail scheduleDetail;
        // make sure the schedule exists before updating
        try {
          scheduleDetail = getSchedule(scheduleId);
        } catch (NotFoundException e) {
          LOG.debug("Failed to get Schedule {}, so its profile metadata will not get updated. " +
                      "Ignoring the message {}: e", scheduleId, message, e);
          return;
        }
        ProgramSchedule schedule = ProgramSchedule.fromScheduleDetail(scheduleDetail);
        collectScheduleProfileMetadata(schedule, getResolvedProfileId(schedule.getProgramId()), updates);
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
      metadataStorage.batch(deletes, MutationOptions.DEFAULT);
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

    for (ScheduleDetail schedule : listSchedules(programId)) {
      ProgramSchedule programSchedule = ProgramSchedule.fromScheduleDetail(schedule);
      collectScheduleProfileMetadata(programSchedule, programProfile, updates);
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
    String profileName = getPreferences(entityId, true).get(SystemArguments.PROFILE_NAME);
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
    String profileName = getPreferences(entityId, false).get(SystemArguments.PROFILE_NAME);
    return profileName == null ? Optional.empty() : Optional.of(ProfileId.fromScopedName(namespaceId, profileName));
  }

  private PreferencesMetadata getPreferencesMetadata(EntityId entityId) throws IOException {
    HttpResponse resp = null;
    String url = getPreferencesMetadataURL(entityId);
    try {
      resp = runHttpGet(url);
    } catch (Exception e) {
      throw new IOException(
        String.format("Failed to get preference metadata for entity %s", entityId.toString()), e);
    }
    return GSON.fromJson(resp.getResponseBodyAsString(), PreferencesMetadata.class);
  }

  private Map<String, String> getPreferences(EntityId entityId, boolean resolved) throws IOException {
    HttpResponse resp;
    String url = getPreferencesURL(entityId, resolved);
    try {
      resp = runHttpGet(url);
    } catch (Exception e) {
      throw new IOException(String.format("Failed to get preference for entity %s", entityId.toString()), e);
    }
    return GSON.fromJson(resp.getResponseBodyAsString(), new TypeToken<Map<String, String>>() { }.getType());
  }

  private String getPreferencesMetadataURL(EntityId entityId) {
    return String.format("%/metadata", getPreferencesURL(entityId, false));
  }

  private String getPreferencesURL(EntityId entityId, boolean resolved) {
    String url;
    switch (entityId.getEntityType()) {
      case INSTANCE:
        url = String.format("preferences");
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        url = String.format("namespaces/%s/preferences", namespaceId.getNamespace());
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        url = String.format("namespaces/%s/apps/%s/preferences",
                            appId.getNamespace(), appId.getApplication());
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        url = String.format("namespaces/%s/apps/%s/%s/%s/preferences",
                            programId.getNamespace(), programId.getApplication(), programId.getType().getCategoryName(),
                            programId.getProgram());
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
    if (resolved) {
      url += "?resolved=true";
    }
    return url;
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

  /**
   * Get the schedule identified by the given schedule id
   */
  private ScheduleDetail getSchedule(ScheduleId scheduleId) throws IOException, NotFoundException {
    String url = String.format("namespaces/%s/apps/%s/versions/%s/schedules/%s",
                               scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getVersion(), scheduleId.getSchedule());
    HttpResponse httpResponse = runHttpGet(url);
    ObjectResponse<ScheduleDetail> objectResponse =
      ObjectResponse.fromJsonBody(httpResponse, new TypeToken<ScheduleDetail>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  /**
   * Get the list of schedules for the given program id
   */
  private List<ScheduleDetail> listSchedules(ProgramId programId) throws IOException {
    String url = String.format("namespaces/%s/apps/%s/versions/%s/schedules",
                               programId.getNamespace(), programId.getApplication(), programId.getVersion());
    HttpResponse httpResponse = null;
    try {
      httpResponse = runHttpGet(url);
    } catch (Exception e) {
      throw new IOException(
        String.format("Failed to list schedules for program %s", programId.toString()), e);
    }
    ObjectResponse<List<ScheduleDetail>> objectResponse = ObjectResponse.fromJsonBody(
      httpResponse, new TypeToken<List<ScheduleDetail>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  /**
   * Get the metadata of the schedule identified by the given schedule id
   */
  private ScheduleMetadata getScheduleMetadata(ScheduleId scheduleId) throws IOException, NotFoundException {
    String url = String.format("namespaces/%s/apps/%s/versions/%s/schedules/%s/metadata",
                               scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getVersion(), scheduleId.getSchedule());
    HttpResponse httpResponse = runHttpGet(url);
    ScheduleMetadata metadata = GSON.fromJson(httpResponse.getResponseBodyAsString(), ScheduleMetadata.class);
    return metadata;
  }


  /**
   * Get the metadata of the application identified by the given id
   */
  private ApplicationMeta getApplicationMeta(ApplicationId appId) throws IOException, NotFoundException {
    String url = String.format("namespaces/%s/apps/%s/versions/%s/metadata",
                               appId.getNamespace(), appId.getApplication(), appId.getVersion());
    HttpResponse resp = runHttpGet(url);
    ApplicationMeta meta = GSON.fromJson(resp.getResponseBodyAsString(), ApplicationMeta.class);
    return meta;
  }

  /**
   * Get the metadata of all applications in the given namespace
   */
  private List<ApplicationMeta> getAllApplicationMetadata(String namespace) throws IOException {
    String url = String.format("namespaces/%s/apps/metadata", namespace);
    HttpResponse resp = null;
    try {
      resp = runHttpGet(url);
    } catch (Exception e) {
      throw new IOException(
        String.format("Failed to get all application metadata in namespace %s", namespace), e);
    }
    ObjectResponse<List<ApplicationMeta>> objectResponse = ObjectResponse.fromJsonBody(
      resp, new TypeToken<List<ApplicationMeta>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }


  /**
   * Make a GET request on the given URL
   */
  private HttpResponse runHttpGet(String url) throws IOException, NotFoundException {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse = remoteClient.execute(requestBuilder.build());
    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new NotFoundException("Request failed with NOT_FOUND:" + httpResponse.getResponseMessage());
    }
    if (httpResponse.getResponseCode() != 200) {
      throw new IOException("Request failed: " + httpResponse.getResponseMessage());
    }
    return httpResponse;
  }
}
