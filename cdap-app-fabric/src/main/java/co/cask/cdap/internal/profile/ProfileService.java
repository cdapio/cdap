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

package co.cask.cdap.internal.profile;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.common.MethodNotAllowedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.app.store.profile.ProfileDataset;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.inject.Inject;

/**
 * This class is to manage profile related functions. It will wrap the {@link ProfileDataset} operation
 * in transaction in each method
 */
public class ProfileService {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileService.class);
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final CConfiguration cConf;

  @Inject
  public ProfileService(CConfiguration cConfiguration,
                        DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
        txClient, NamespaceId.SYSTEM,
        Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.cConf = cConfiguration;
  }

  /**
   * Get the profile information about the given profile
   *
   * @param profileId the id of the profile to look up
   * @return the profile information about the given profile
   * @throws NotFoundException if the profile is not found
   */
  public Profile getProfile(ProfileId profileId) throws NotFoundException {
    return Transactionals.execute(transactional, context -> {
      return getProfileDataset(context).getProfile(profileId);
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
    return Transactionals.execute(transactional, context -> {
      return getProfileDataset(context).getProfiles(namespaceId, includeSystem);
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
    Transactionals.execute(transactional, context -> {
      ProfileDataset dataset = getProfileDataset(context);
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
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).createIfNotExists(profileId, profile);
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
    throws NotFoundException, ProfileConflictException, MethodNotAllowedException {
    if (profileId.equals(ProfileId.NATIVE)) {
      throw new MethodNotAllowedException(String.format("Profile Native %s cannot be deleted.",
                                                        profileId.getScopedName()));
    }
    Transactionals.execute(transactional, context -> {
      ProfileDataset profileDataset = getProfileDataset(context);
      Profile profile = profileDataset.getProfile(profileId);
      AppMetadataStore appMetadataStore = AppMetadataStore.create(cConf, context, datasetFramework);
      deleteProfile(profileDataset, appMetadataStore, profileId, profile);
    }, NotFoundException.class, ProfileConflictException.class);
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
    Transactionals.execute(transactional, context -> {
      ProfileDataset profileDataset = getProfileDataset(context);
      AppMetadataStore appMetadataStore = AppMetadataStore.create(cConf, context, datasetFramework);
      List<Profile> profiles = profileDataset.getProfiles(namespaceId, false);
      for (Profile profile : profiles) {
        deleteProfile(profileDataset, appMetadataStore, namespaceId.profile(profile.getName()), profile);
      }
    }, ProfileConflictException.class, NotFoundException.class);
  }

  /**
   * Delete all profiles. This method can only be used at unit tests
   */
  @VisibleForTesting
  public void clear() {
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).deleteAllProfiles();
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
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).enableProfile(profileId);
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
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).disableProfile(profileId);
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
    return Transactionals.execute(transactional, context -> {
      return getProfileDataset(context).getProfileAssignments(profileId);
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
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).addProfileAssignment(profileId, entityId);
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
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).removeProfileAssignment(profileId, entityId);
    }, NotFoundException.class);
  }

  private ProfileDataset getProfileDataset(DatasetContext context) {
    return ProfileDataset.get(context, datasetFramework);
  }

  private void deleteProfile(ProfileDataset profileDataset, AppMetadataStore appMetadataStore, ProfileId profileId,
                             Profile profile) throws ProfileConflictException, NotFoundException {
    // The profile status must be DISABLED
    if (profile.getStatus() == ProfileStatus.ENABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s in namespace %s is currently enabled. A profile can " +
                        "only be deleted if it is disabled", profileId.getProfile(), profileId.getNamespace()),
        profileId);
    }

    // There must be no assigments to this profile
    Set<EntityId> assignments = profileDataset.getProfileAssignments(profileId);
    if (!assignments.isEmpty()) {
      throw new ProfileConflictException(
        String.format("This profile %s is still assigned to %d entities, like %s. " +
                        "Please delete all assignments before deleting the profile.",
                      profileId.toString(), assignments.size(), assignments.iterator().next()),
        profileId);
    }

    // There must be no running programs using the profile
    Map<ProgramRunId, RunRecordMeta> activeRuns;
    Predicate<RunRecordMeta> runRecordMetaPredicate = runRecordMeta -> {
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
    if (!activeRuns.isEmpty()) {
      throw new ProfileConflictException(
        String.format("The profile %s is in use by %d active runs, like %s. Please stop all active runs " +
                        "before deleting the profile.",
                      profileId.toString(), activeRuns.size(), activeRuns.keySet().iterator().next()),
        profileId);
    }

    // delete the profile
    profileDataset.deleteProfile(profileId);
  }
}
