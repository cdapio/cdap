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

package co.cask.cdap.internal.app.store.profile;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset for profile information. This dataset does not wrap its operations in a transaction. It is up to the caller
 * to decide what operations belong in a transaction.
 * This dataset uses an underlying MetadataStoreDataset and uses its default COLUMN to store all the results.
 * There are two kinds of row key prefixes to store profile and entity assignments.
 * The row key prefix for the profile is "profile", the row key prefix for the entity assignment is "proAssign".
 *
 * The table will look like:
 * row key                                                              column -> value
 * "profile"[namespace][profile-id]                                     c -> Profile{@link Profile}
 * "proAssign"[namespace][profile-id][entity-id]                        c -> EntityId{@link EntityId}
 */
public class ProfileDataset {
  private static final DatasetId DATASET_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  // this prefix is used to construct a row key to store the profile information about profile
  private static final String PROFILE_PREFIX = "profile";
  // this prefix is used to construct a row key to store the entities that are associated to a particular profile
  private static final String INDEX_PREFIX = "proAssign";
  private static final DatasetProperties TABLE_PROPERTIES =
    TableProperties.builder().setConflictDetection(ConflictDetection.COLUMN).build();

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter()).create();

  private final MetadataStoreDataset table;

  private ProfileDataset(Table table) {
    this.table = new MetadataStoreDataset(table, GSON);
  }

  public static ProfileDataset get(DatasetContext datasetContext, DatasetFramework dsFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext,
                                                    dsFramework, DATASET_ID, Table.class.getName(), TABLE_PROPERTIES);
      return new ProfileDataset(table);
    } catch (IOException | DatasetManagementException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the profile information about the given profile
   *
   * @param profileId the id of the profile to look up
   * @return the profile information about the given profile
   * @throws NotFoundException if the profile is not found
   */
  public Profile getProfile(ProfileId profileId) throws NotFoundException {
    Profile profile = table.get(getProfileRowKey(profileId), Profile.class);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }
    return profile;
  }

  /**
   * Get the profile information in the given namespace
   *
   * @param namespaceId the id of the profile to look up
   * @param includeSystem whether to include profiles in system namespace
   * @return the list of profiles which is in this namespace
   */
  public List<Profile> getProfiles(NamespaceId namespaceId, boolean includeSystem) {
    List<Profile> profiles = new ArrayList<>(table.list(getProfileRowKeyPrefix(namespaceId), Profile.class));
    if (includeSystem && !namespaceId.equals(NamespaceId.SYSTEM)) {
      profiles.addAll(table.list(getProfileRowKeyPrefix(NamespaceId.SYSTEM), Profile.class));
    }
    return Collections.unmodifiableList(profiles);
  }

  /**
   * Save the profile to the profile store. By default the profile status will be enabled.
   *
   * @param profileId the id of the profile to save
   * @param profile the information of the profile
   */
  public void saveProfile(ProfileId profileId, Profile profile) {
    MDSKey rowKey = getProfileRowKey(profileId);
    Profile oldProfile = table.get(rowKey, Profile.class);
    table.write(
      rowKey, new Profile(profile.getName(), profile.getLabel(), profile.getDescription(), profile.getScope(),
                          oldProfile == null ? ProfileStatus.ENABLED : oldProfile.getStatus(),
                          profile.getProvisioner(),
                          oldProfile == null ? profile.getCreatedTsSeconds() : oldProfile.getCreatedTsSeconds()));
  }

  /**
   * Add a profile if it does not exist in the store
   *
   * @param profileId the id of the profile to add
   * @param profile the information of the profile
   */
  public void createIfNotExists(ProfileId profileId, Profile profile) {
    MDSKey rowKey = getProfileRowKey(profileId);
    Profile oldProfile = table.get(rowKey, Profile.class);
    if (oldProfile != null) {
      return;
    }
    table.write(rowKey, new Profile(profile.getName(), profile.getLabel(), profile.getDescription(), profile.getScope(),
                                    ProfileStatus.ENABLED, profile.getProvisioner()));
  }

  /**
   * Deletes the profile from the profile store
   *
   * @param profileId the id of the profile to delete
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is enabled
   */
  public void deleteProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException {
    MDSKey rowKey = getProfileRowKey(profileId);
    Profile value = table.get(rowKey, Profile.class);
    if (value == null) {
      throw new NotFoundException(profileId);
    }
    if (value.getStatus() == ProfileStatus.ENABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s in namespace %s is currently enabled. A profile can " +
                        "only be deleted if it is disabled", profileId.getProfile(), profileId.getNamespace()),
        profileId);
    }
    table.delete(getProfileRowKey(profileId));
  }

  /**
   * Delete all profiles in a given namespace.
   */
  @VisibleForTesting
  public void deleteAllProfiles() {
    table.deleteAll(getAllProfileRowKeyPrefix());
  }

  /**
   * Enable the profile. After the profile is enabled, any program/schedule can be associated with this profile.
   *
   * @param profileId the id of the profile to enable
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is already enabled
   */
  public void enableProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException {
    changeProfileStatus(profileId, ProfileStatus.ENABLED);
  }

  /**
   * Disable the profile. After the profile is disabled, any program/schedule cannot be associated with this profile.
   *
   * @param profileId the id of the profile to disable
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is already disabled
   */
  public void disableProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException {
    changeProfileStatus(profileId, ProfileStatus.DISABLED);
  }

  private void changeProfileStatus(ProfileId profileId,
                                   ProfileStatus expectedStatus) throws NotFoundException, ProfileConflictException {
    MDSKey rowKey = getProfileRowKey(profileId);
    Profile oldProfile = table.get(rowKey, Profile.class);
    if (oldProfile == null) {
      throw new NotFoundException(profileId);
    }
    if (oldProfile.getStatus() == expectedStatus) {
      throw new ProfileConflictException(
        String.format("Profile %s already %s", profileId.getProfile(), expectedStatus.toString()), profileId);
    }
    table.write(rowKey, new Profile(oldProfile.getName(), oldProfile.getLabel(), oldProfile.getDescription(),
                                    oldProfile.getScope(), expectedStatus, oldProfile.getProvisioner(),
                                    oldProfile.getCreatedTsSeconds()));
  }

  /**
   * Get assignments with the profile.
   *
   * @param profileId the profile id
   * @return the set of entities that the profile is assigned to
   * @throws NotFoundException if the profile is not found
   */
  public Set<EntityId> getProfileAssignments(ProfileId profileId) throws NotFoundException {
    Profile profile = table.get(getProfileRowKey(profileId), Profile.class);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }

    return Collections.unmodifiableSet(new HashSet<>(table.list(getEntityIndexRowKeyPrefix(profileId),
                                                                EntityId.class)));
  }

  /**
   * Add an assignment to the profile. Assignment can only be added if the profile is ENABLED
   *
   * @param profileId the profile id
   * @param entityId the entity to add to the assgiment
   */
  public void addProfileAssignment(ProfileId profileId,
                                   EntityId entityId) throws ProfileConflictException, NotFoundException {
    Profile profile = table.get(getProfileRowKey(profileId), Profile.class);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }
    if (profile.getStatus() == ProfileStatus.DISABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s is DISABLED. No entity can be assigned to it.", profileId.getProfile()), profileId);
    }
    table.write(getEntityIndexRowKey(profileId, entityId), entityId);
  }

  /**
   * Remove an assignment from the profile.
   *
   * @param profileId the profile id
   * @param entityId the entity to remove from the assignment
   * @throws NotFoundException if the profile is not found
   */
  public void removeProfileAssignment(ProfileId profileId, EntityId entityId) throws NotFoundException {
    Profile profile = table.get(getProfileRowKey(profileId), Profile.class);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }
    table.delete(getEntityIndexRowKey(profileId, entityId));
  }

  private MDSKey getAllProfileRowKeyPrefix() {
    return getRowKey(PROFILE_PREFIX, null, null, null);
  }

  private MDSKey getProfileRowKeyPrefix(NamespaceId namespaceId) {
    return getRowKey(PROFILE_PREFIX, namespaceId, null, null);
  }

  private MDSKey getProfileRowKey(ProfileId profileId) {
    return getRowKey(PROFILE_PREFIX, profileId.getNamespaceId(), profileId.getProfile(), null);
  }

  private MDSKey getEntityIndexRowKeyPrefix(ProfileId profileId) {
    return getRowKey(INDEX_PREFIX, profileId.getNamespaceId(), profileId.getProfile(), null);
  }

  private MDSKey getEntityIndexRowKey(ProfileId profileId, EntityId entityId) {
    return getRowKey(INDEX_PREFIX, profileId.getNamespaceId(), profileId.getProfile(), entityId);
  }

  private MDSKey getRowKey(String prefix, @Nullable NamespaceId namespaceId,
                           @Nullable String profileName, @Nullable EntityId entityId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(prefix);
    if (namespaceId != null) {
      builder.add(namespaceId.getEntityName());
    }
    if (profileName != null) {
      builder.add(profileName);
    }
    if (entityId != null) {
      entityId.toIdParts().forEach(builder::add);
    }
    return builder.build();
  }
}
