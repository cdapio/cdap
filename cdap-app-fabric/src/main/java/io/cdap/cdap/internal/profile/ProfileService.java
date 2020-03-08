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

package io.cdap.cdap.internal.profile;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.common.MethodNotAllowedException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.app.store.profile.ProfileStore;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.inject.Inject;

/**
 * This class is to manage profile related functions. It will wrap the {@link ProfileStore} operation
 * in transaction in each method
 */
public class ProfileService {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileService.class);
  private final MetricsSystemClient metricsSystemClient;
  private final TransactionRunner transactionRunner;

  @Inject
  public ProfileService(MetricsSystemClient metricsSystemClient,
                        TransactionRunner transactionRunner) {
    this.metricsSystemClient = metricsSystemClient;
    this.transactionRunner = transactionRunner;
  }

  /**
   * Get the profile information about the given profile
   *
   * @param profileId the id of the profile to look up
   * @return the profile information about the given profile
   * @throws NotFoundException if the profile is not found
   */
  public Profile getProfile(ProfileId profileId) throws NotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      return ProfileStore.get(context).getProfile(profileId);
    }, NotFoundException.class);
  }

  /**
   * Get the profile information about the given profile with property overrides. If a non-editable property is
   * in the overrides, it will be ignored, but a message will be logged.
   *
   * @param profileId the id of the profile to look up
   * @param overrides overrides to the profile properties
   * @return the profile information about the given profile
   * @throws NotFoundException if the profile is not found
   */
  public Profile getProfile(ProfileId profileId, Map<String, String> overrides) throws NotFoundException {
    Profile storedProfile = getProfile(profileId);

    List<ProvisionerPropertyValue> properties = new ArrayList<>();
    Set<String> remainingOverrides = new HashSet<>(overrides.keySet());

    // add all  properties from the stored profile
    for (ProvisionerPropertyValue storedProperty : storedProfile.getProvisioner().getProperties()) {
      String propertyName = storedProperty.getName();
      String storedVal = storedProperty.getValue();
      if (!storedProperty.isEditable()) {
        if (overrides.containsKey(propertyName)) {
          LOG.info("Profile property {} cannot be edited. The original value will be used.", propertyName);
        }
        properties.add(storedProperty);
      } else {
        String val = overrides.getOrDefault(propertyName, storedVal);
        properties.add(new ProvisionerPropertyValue(propertyName, val, true));
      }
      remainingOverrides.remove(propertyName);
    }

    // add all remaining overrides
    for (String propertyName : remainingOverrides) {
      properties.add(new ProvisionerPropertyValue(propertyName, overrides.get(propertyName), true));
    }
    ProvisionerInfo provisionerInfo = new ProvisionerInfo(storedProfile.getProvisioner().getName(), properties);
    return new Profile(storedProfile.getName(), storedProfile.getLabel(), storedProfile.getDescription(),
                       storedProfile.getScope(), storedProfile.getStatus(), provisionerInfo,
                       storedProfile.getCreatedTsSeconds());
  }

  /**
   * Get the profile information in the given namespace
   *
   * @param namespaceId the id of the profile to look up
   * @param includeSystem whether to include profiles in system namespace
   * @return the list of profiles which is in this namespace
   */
  public List<Profile> getProfiles(NamespaceId namespaceId, boolean includeSystem) {
    return TransactionRunners.run(transactionRunner, context -> {
      return ProfileStore.get(context).getProfiles(namespaceId, includeSystem);
    });
  }

  /**
   * Save the profile to the profile store. By default the profile status will be enabled.
   *
   * @param profileId the id of the profile to save
   * @param profile the information of the profile
   * @throws MethodNotAllowedException if trying to update the Native profile
   */
  public void saveProfile(ProfileId profileId, Profile profile) throws MethodNotAllowedException {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore dataset = ProfileStore.get(context);
      if (profileId.equals(ProfileId.NATIVE)) {
        try {
          dataset.getProfile(profileId);
          throw new MethodNotAllowedException(String.format("Profile Native %s already exists. It cannot be updated",
                                                            profileId.getScopedName()));
        } catch (NotFoundException e) {
          // if native profile is not found, we can add it to the dataset
        }
      }
      dataset.saveProfile(profileId, profile);
    }, MethodNotAllowedException.class);
  }

  /**
   * Add a profile if it does not exist in the store
   *
   * @param profileId the id of the profile to add
   * @param profile the information of the profile
   */
  public void createIfNotExists(ProfileId profileId, Profile profile) {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore.get(context).createIfNotExists(profileId, profile);
    });
  }

  /**
   * Deletes the profile from the profile store. Native profile cannot be deleted.
   * Other profile deletion must satisfy the following:
   * 1. Profile must exist and must be DISABLED
   * 2. Profile must not be assigned to any entities. Profiles can be assigned to an entity by setting a preference
   *    or a schedule property.
   * 3. There must be no active program runs using this profile
   *
   * @param profileId the id of the profile to delete
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is enabled
   * @throws MethodNotAllowedException if trying to delete the Native profile
   */
  public void deleteProfile(ProfileId profileId)
    throws MethodNotAllowedException, NotFoundException, ProfileConflictException {
    if (profileId.equals(ProfileId.NATIVE)) {
      throw new MethodNotAllowedException(String.format("Profile Native %s cannot be deleted.",
                                                        profileId.getScopedName()));
    }
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore profileStore = ProfileStore.get(context);
      Profile profile = profileStore.getProfile(profileId);
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      deleteProfile(profileStore, appMetadataStore, profileId, profile);
    }, NotFoundException.class, ProfileConflictException.class);

    deleteMetrics(profileId);
  }

  /**
   * Delete all profiles in a given namespace. Deleting all profiles in SYSTEM namespace is not allowed.
   *
   * @param namespaceId the id of the namespace
   */
  public void deleteAllProfiles(NamespaceId namespaceId)
    throws MethodNotAllowedException, NotFoundException, ProfileConflictException {
    if (namespaceId.equals(NamespaceId.SYSTEM)) {
      throw new MethodNotAllowedException("Deleting all system profiles is not allowed.");
    }
    List<ProfileId> deleted = new ArrayList<>();
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore profileStore = ProfileStore.get(context);
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      List<Profile> profiles = profileStore.getProfiles(namespaceId, false);
      for (Profile profile : profiles) {
        ProfileId profileId = namespaceId.profile(profile.getName());
        deleteProfile(profileStore, appMetadataStore, profileId, profile);
        deleted.add(profileId);
      }
    }, ProfileConflictException.class, NotFoundException.class);
    // delete the metrics
    for (ProfileId profileId : deleted) {
      deleteMetrics(profileId);
    }
  }

  /**
   * Delete all profiles. This method can only be used at unit tests
   */
  @VisibleForTesting
  public void clear() {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore.get(context).deleteAllProfiles();
    });
  }

  /**
   * Enable the profile. After the profile is enabled, any program/schedule can be associated with this profile.
   *
   * @param profileId the id of the profile to enable
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is already enabled
   */
  public void enableProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore.get(context).enableProfile(profileId);
    }, NotFoundException.class, ProfileConflictException.class);
  }

  /**
   * Disable the profile. After the profile is disabled, any program/schedule cannot be associated with this profile.
   *
   * @param profileId the id of the profile to disable
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is already disabled
   * @throws MethodNotAllowedException if trying to disable the native profile
   */
  public void disableProfile(ProfileId profileId)
    throws NotFoundException, ProfileConflictException, MethodNotAllowedException {
    if (profileId.equals(ProfileId.NATIVE)) {
      throw new MethodNotAllowedException(String.format("Cannot change status for Profile Native %s, " +
                                                          "it should always be ENABLED", profileId.getScopedName()));
    }
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore.get(context).disableProfile(profileId);
    }, NotFoundException.class, ProfileConflictException.class);
  }

  /**
   * Get assignments with the profile.
   *
   * @param profileId the profile id
   * @return the entities that the profile is assigned to
   * @throws NotFoundException if the profile is not found
   */
  public Set<EntityId> getProfileAssignments(ProfileId profileId) throws NotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      return ProfileStore.get(context).getProfileAssignments(profileId);
    }, NotFoundException.class);
  }

  /**
   * Add an assignment to the profile.
   *
   * @param profileId the profile id
   * @param entityId the entity to add to the assignments
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is disabled
   */
  public void addProfileAssignment(ProfileId profileId,
                                   EntityId entityId) throws NotFoundException, ProfileConflictException {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore.get(context).addProfileAssignment(profileId, entityId);
    }, NotFoundException.class, ProfileConflictException.class);
  }

  /**
   * Remove an assignment from the profile.
   *
   * @param profileId the profile id
   * @param entityId the entity to remove from the assignments
   * @throws NotFoundException if the profile is not found
   */
  public void removeProfileAssignment(ProfileId profileId, EntityId entityId) throws NotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      ProfileStore.get(context).removeProfileAssignment(profileId, entityId);
    }, NotFoundException.class);
  }

  private void deleteProfile(ProfileStore profileStore, AppMetadataStore appMetadataStore, ProfileId profileId,
                             Profile profile) throws ProfileConflictException, NotFoundException, IOException {
    // The profile status must be DISABLED
    if (profile.getStatus() == ProfileStatus.ENABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s in namespace %s is currently enabled. A profile can " +
                        "only be deleted if it is disabled", profileId.getProfile(), profileId.getNamespace()),
        profileId);
    }

    // There must be no assignments to this profile
    Set<EntityId> assignments = profileStore.getProfileAssignments(profileId);
    int numAssignments = assignments.size();
    if (numAssignments > 0) {
      String firstEntity = getUserFriendlyEntityStr(assignments.iterator().next());
      String countStr = getCountStr(numAssignments, "entity", "entities");
      throw new ProfileConflictException(
        String.format("Profile '%s' is still assigned to %s%s. " +
                        "Please delete all assignments before deleting the profile.",
                      profileId.getProfile(), firstEntity, countStr),
        profileId);
    }

    // There must be no running programs using the profile
    Map<ProgramRunId, RunRecordDetail> activeRuns;
    Predicate<RunRecordDetail> runRecordMetaPredicate = runRecordMeta -> {
      // the profile comes in system arguments with the scoped name
      String scopedName = runRecordMeta.getSystemArgs().get(SystemArguments.PROFILE_NAME);
      return scopedName != null && scopedName.equals(profileId.getScopedName());
    };
    if (profileId.getNamespaceId().equals(NamespaceId.SYSTEM)) {
      activeRuns = appMetadataStore.getActiveRuns(runRecordMetaPredicate);
    } else {
      activeRuns = appMetadataStore.getActiveRuns(Collections.singleton(profileId.getNamespaceId()),
                                                  runRecordMetaPredicate);
    }
    int numRuns = activeRuns.size();
    if (numRuns > 0) {
      String firstRun = activeRuns.keySet().iterator().next().toString();
      String countStr = getCountStr(numRuns, "run", "runs");
      throw new ProfileConflictException(
        String.format("Profile '%s' is in use by run %s%s. Please stop all active runs " +
                        "before deleting the profile.",
                      profileId.toString(), firstRun, countStr),
        profileId);
    }

    // delete the profile
    profileStore.deleteProfile(profileId);
  }

  private String getUserFriendlyEntityStr(EntityId entityId) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        return "the system instance";
      case NAMESPACE:
        return String.format("namespace '%s'", entityId.getEntityName());
      case APPLICATION:
        ApplicationId applicationId = (ApplicationId) entityId;
        return String.format("application '%s' in namespace '%s'", applicationId.getApplication(),
                             applicationId.getNamespace());
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        return String.format("%s '%s' in namespace '%s'", programId.getType().name().toLowerCase(),
                             programId.getProgram(), programId.getNamespace());
    }
    return entityId.toString();
  }

  private String getCountStr(int count, String singular, String plural) {
    if (count == 1) {
      return "";
    } else if (count == 2) {
      return String.format(" and 1 other %s", singular);
    }
    return String.format(" and %d other %s", count - 1, plural);
  }

  /**
   * Delete the metrics for a profile.
   *
   * @param profileId the profile to delete metrics for.
   */
  private void deleteMetrics(ProfileId profileId) {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name());
    tags.put(Constants.Metrics.Tag.PROFILE, profileId.getProfile());
    // if the profile is in user scope, we need to add the namespace info to distinguish the profile
    if (profileId.getScope().equals(EntityScope.USER)) {
      tags.put(Constants.Metrics.Tag.NAMESPACE, profileId.getNamespace());
    }
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.emptySet(), tags,
                                                          new ArrayList<>(tags.keySet()));
    try {
      metricsSystemClient.delete(deleteQuery);
    } catch (IOException e) {
      LOG.warn("Failed to delete metrics for profile {}", profileId, e);
    }
  }
}
