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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.store.profile.ProfileDataset;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
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
import javax.inject.Inject;

/**
 * This class is to manage profile related functions. For any store related operations,
 * it will directly get any underlying table.
 */
public class ProfileService {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileService.class);
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public ProfileService(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
        txClient, NamespaceId.SYSTEM,
        Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
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
    return new Profile(storedProfile.getName(), storedProfile.getDescription(),
                       storedProfile.getScope(), storedProfile.getStatus(), provisionerInfo);
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
   */
  public void saveProfile(ProfileId profileId, Profile profile) {
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).saveProfile(profileId, profile);
    });
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
   * Deletes the profile from the profile store
   *
   * @param profileId the id of the profile to delete
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is enabled
   */
  public void deleteProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException {
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).deleteProfile(profileId);
    }, NotFoundException.class, ProfileConflictException.class);
  }

  /**
   * Delete all profiles in a given namespace.
   *
   * @param namespaceId the id of the namespace
   */
  public void deleteAllProfiles(NamespaceId namespaceId) {
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).deleteAllProfiles(namespaceId);
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
   */
  public void disableProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException {
    Transactionals.execute(transactional, context -> {
      getProfileDataset(context).disableProfile(profileId);
    }, NotFoundException.class, ProfileConflictException.class);
  }

  private ProfileDataset getProfileDataset(DatasetContext context) {
    return ProfileDataset.get(context, datasetFramework);
  }
}
