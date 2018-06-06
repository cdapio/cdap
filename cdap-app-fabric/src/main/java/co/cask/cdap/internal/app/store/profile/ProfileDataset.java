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
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Dataset for profile information. This dataset does not wrap its operations in a transaction. It is up to the caller
 * to decide what operations belong in a transaction.
 */
public class ProfileDataset {
  private static final DatasetId DATASET_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final String PROFILE_PREFIX = "profile";
  private static final DatasetProperties TABLE_PROPERTIES =
    TableProperties.builder().setConflictDetection(ConflictDetection.COLUMN).build();
  private static final Gson GSON = new Gson();

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
    Profile profile = table.get(getRowKey(profileId), Profile.class);
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
      List<Profile> profiles = new ArrayList<>(table.list(getRowKey(namespaceId), Profile.class));
      if (includeSystem && !namespaceId.equals(NamespaceId.SYSTEM)) {
        profiles.addAll(table.list(getRowKey(NamespaceId.SYSTEM), Profile.class));
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
    MDSKey rowKey = getRowKey(profileId);
    Profile oldProfile = table.get(rowKey, Profile.class);
    table.write(
      rowKey, new Profile(profile.getName(), profile.getDescription(), profile.getScope(),
                          oldProfile == null ? ProfileStatus.ENABLED : oldProfile.getStatus(),
                          profile.getProvisioner()));
  }

  /**
   * Add a profile if it does not exist in the store
   *
   * @param profileId the id of the profile to add
   * @param profile the information of the profile
   */
  public void createIfNotExists(ProfileId profileId, Profile profile) {
    MDSKey rowKey = getRowKey(profileId);
    Profile oldProfile = table.get(rowKey, Profile.class);
    if (oldProfile != null) {
      return;
    }
    table.write(rowKey, new Profile(profile.getName(), profile.getDescription(), profile.getScope(),
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
    MDSKey rowKey = getRowKey(profileId);
    Profile value = table.get(rowKey, Profile.class);
    if (value == null) {
      throw new NotFoundException(profileId);
    }
    // TODO: https://issues.cask.co/browse/CDAP-13514, check for associations of the profile before deleting
    if (value.getStatus() == ProfileStatus.ENABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s in namespace %s is currently enabled. A profile can " +
                        "only be deleted if it is disabled", profileId.getProfile(), profileId.getNamespace()),
        profileId);
    }
    table.delete(getRowKey(profileId));
  }

  /**
   * Delete all profiles in a given namespace.
   *
   * @param namespaceId the id of the namespace
   */
  public void deleteAllProfiles(NamespaceId namespaceId) {
    table.deleteAll(getRowKey(namespaceId));
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
    MDSKey rowKey = getRowKey(profileId);
    Profile oldProfile = table.get(rowKey, Profile.class);
    if (oldProfile == null) {
      throw new NotFoundException(profileId);
    }
    if (oldProfile.getStatus() == expectedStatus) {
      throw new ProfileConflictException(
        String.format("Profile %s already %s", profileId.getProfile(), expectedStatus.toString()), profileId);
    }
    table.write(rowKey, new Profile(oldProfile.getName(), oldProfile.getDescription(), oldProfile.getScope(),
                                    expectedStatus, oldProfile.getProvisioner()));
  }

  private MDSKey getRowKey(NamespaceId namespaceId) {
    return getRowKey(namespaceId, null);
  }

  private MDSKey getRowKey(ProfileId profileId) {
    return getRowKey(profileId.getNamespaceId(), profileId.getProfile());
  }

  private MDSKey getRowKey(NamespaceId namespaceId, @Nullable String profileName) {
    MDSKey.Builder builder = new MDSKey.Builder().add(PROFILE_PREFIX).add(namespaceId.getEntityName());
    if (profileName != null) {
      builder.add(profileName);
    }
    return builder.build();
  }
}
