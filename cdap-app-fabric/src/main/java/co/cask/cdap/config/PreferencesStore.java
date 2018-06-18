/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.profile.ProfileDataset;
import co.cask.cdap.internal.profile.ProfileMetadataPublisher;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Use config dataset to perform operations around preference.
 */
public class PreferencesStore {
  private static final String PREFERENCES_CONFIG_TYPE = "preferences";
  // Namespace where instance level properties are stored
  private static final String EMPTY_NAMESPACE = "";

  // Id for Properties config stored at the instance level
  private static final String INSTANCE_PROPERTIES = "instance";

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final ProfileMetadataPublisher profileMetadataPublisher;

  @Inject
  public PreferencesStore(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                          ProfileMetadataPublisher profileMetadataPublisher) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
        txClient, NamespaceId.SYSTEM,
        Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.profileMetadataPublisher = profileMetadataPublisher;
  }

  private Map<String, String> getConfigProperties(String namespace, String id) {
    return Transactionals.execute(transactional, context -> {
      Map<String, String> value = new HashMap<>();
      try {
        Config config = ConfigDataset.get(context, datasetFramework).get(namespace, PREFERENCES_CONFIG_TYPE, id);
        value.putAll(config.getProperties());
      } catch (ConfigNotFoundException e) {
        //no-op - return empty map
      }
      return value;
    });
  }

  private void setConfig(String namespace, EntityId entityId, String id,
                         Map<String, String> propertyMap) throws NotFoundException, ProfileConflictException {
    Config config = new Config(id, propertyMap);
    validateAndSetPreferences(namespace, entityId, config);
  }

  private void deleteConfig(String namespace, EntityId entityId, String id) {
    boolean profileExists = Transactionals.execute(transactional, context -> {
      try {
        ConfigDataset configDataset = ConfigDataset.get(context, datasetFramework);
        boolean exist = configDataset.get(namespace, PREFERENCES_CONFIG_TYPE, id)
                          .getProperties().containsKey(SystemArguments.PROFILE_NAME);
        configDataset.delete(namespace, PREFERENCES_CONFIG_TYPE, id);
        return exist;
      } catch (ConfigNotFoundException e) {
        return false;
      }
    });

    // remove the profile metadata since there is profile information in the preference
    if (profileExists) {
      profileMetadataPublisher.updateProfileMetadata(entityId);
    }
  }

  public Map<String, String> getProperties() {
    return getConfigProperties(EMPTY_NAMESPACE, getMultipartKey(INSTANCE_PROPERTIES));
  }

  public Map<String, String> getProperties(String namespace) {
    return getConfigProperties(namespace, getMultipartKey(namespace));
  }

  public Map<String, String> getProperties(String namespace, String appId) {
    return getConfigProperties(namespace, getMultipartKey(namespace, appId));
  }

  public Map<String, String> getProperties(String namespace, String appId, String programType, String programId) {
    return getConfigProperties(namespace, getMultipartKey(namespace, appId, programType, programId));
  }

  public Map<String, String> getResolvedProperties() {
    return getProperties();
  }

  public Map<String, String> getResolvedProperties(String namespace) {
    Map<String, String> propMap = getResolvedProperties();
    propMap.putAll(getProperties(namespace));
    return propMap;
  }

  public Map<String, String> getResolvedProperties(String namespace, String appId) {
    Map<String, String> propMap = getResolvedProperties(namespace);
    propMap.putAll(getProperties(namespace, appId));
    return propMap;
  }

  public Map<String, String> getResolvedProperties(String namespace, String appId, String programType,
                                                   String programId) {
    Map<String, String> propMap = getResolvedProperties(namespace, appId);
    propMap.putAll(getProperties(namespace, appId, programType, programId));
    return propMap;
  }

  public void setProperties(Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(EMPTY_NAMESPACE, new InstanceId(EMPTY_NAMESPACE), getMultipartKey(INSTANCE_PROPERTIES), propMap);
  }

  public void setProperties(String namespace,
                            Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(namespace, new NamespaceId(namespace), getMultipartKey(namespace), propMap);
  }

  public void setProperties(String namespace, String appId, Map<String, String> propMap)
    throws NotFoundException, ProfileConflictException {
    setConfig(namespace, new ApplicationId(namespace, appId), getMultipartKey(namespace, appId), propMap);
  }

  public void setProperties(String namespace, String appId, String programType, String programId,
                            Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(namespace, new ProgramId(namespace, appId, programType, programId),
              getMultipartKey(namespace, appId, programType, programId), propMap);
  }

  public void deleteProperties() {
    deleteConfig(EMPTY_NAMESPACE, new InstanceId(EMPTY_NAMESPACE), getMultipartKey(INSTANCE_PROPERTIES));
  }

  public void deleteProperties(String namespace) {
    deleteConfig(namespace, new NamespaceId(namespace), getMultipartKey(namespace));
  }

  public void deleteProperties(String namespace, String appId) {
    deleteConfig(namespace, new ApplicationId(namespace, appId), getMultipartKey(namespace, appId));
  }

  public void deleteProperties(String namespace, String appId, String programType, String programId) {
    deleteConfig(namespace, new ProgramId(namespace, appId, programType, programId),
                 getMultipartKey(namespace, appId, programType, programId));
  }

  private String getMultipartKey(String... parts) {
    int sizeOfParts = 0;
    for (String part : parts) {
      sizeOfParts += part.length();
    }

    byte[] result = new byte[sizeOfParts + (parts.length * Bytes.SIZEOF_INT)];

    int offset = 0;
    for (String part : parts) {
      Bytes.putInt(result, offset, part.length());
      offset += Bytes.SIZEOF_INT;
      Bytes.putBytes(result, offset, part.getBytes(), 0, part.length());
      offset += part.length();
    }
    return Bytes.toString(result);
  }

  /**
   * Validate the profile status is enabled and set the preferences in same transaction
   */
  private void validateAndSetPreferences(String namespace, EntityId entityId,
                                         Config config) throws NotFoundException, ProfileConflictException {
    boolean profileExists = Transactionals.execute(transactional, context -> {
      ProfileDataset profileDataset = ProfileDataset.get(context, datasetFramework);
      ConfigDataset configDataset = ConfigDataset.get(context, datasetFramework);
      NamespaceId namespaceId = EMPTY_NAMESPACE.equals(namespace) ? NamespaceId.SYSTEM : new NamespaceId(namespace);
      boolean exists = validateProfileProperties(namespaceId, config.getProperties(), profileDataset);
      configDataset.createOrUpdate(namespace, PREFERENCES_CONFIG_TYPE, config);
      return exists;
    }, NotFoundException.class, ProfileConflictException.class);

    // publish the necessary metadata change if the profile exists in the property and we have successfully updated
    // the preference store
    if (profileExists) {
      profileMetadataPublisher.updateProfileMetadata(entityId);
    }
  }

  /**
   * Validate and check whether the profile information is in the property
   */
  private boolean validateProfileProperties(NamespaceId namespaceId, Map<String, String> propertyMap,
                                              ProfileDataset profileDataset)
    throws NotFoundException, ProfileConflictException {
    Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(namespaceId, propertyMap);
    if (!profile.isPresent()) {
      return false;
    }

    ProfileId profileId = profile.get();
    if (profileDataset.getProfile(profileId).getStatus() == ProfileStatus.DISABLED) {
      throw new ProfileConflictException(String.format("Profile %s in namespace %s is disabled. It cannot be " +
                                                         "assigned to any programs or schedules",
                                                       profileId.getProfile(), profileId.getNamespace()), profileId);
    }
    return true;
  }
}
