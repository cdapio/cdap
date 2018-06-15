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
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
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

  @Inject
  public PreferencesStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
        txClient, NamespaceId.SYSTEM,
        Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
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

  private void setConfig(String namespace, String id,
                         Map<String, String> propertyMap) throws NotFoundException, ProfileConflictException {
    Config config = new Config(id, propertyMap);
    validateAndSetPreferences(namespace, config);
  }

  private void deleteConfig(String namespace, String id) {
    Transactionals.execute(transactional, context -> {
      try {
        ConfigDataset.get(context, datasetFramework).delete(namespace, PREFERENCES_CONFIG_TYPE, id);
      } catch (ConfigNotFoundException e) {
        //no-op
      }
    });
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
    setConfig(EMPTY_NAMESPACE, getMultipartKey(INSTANCE_PROPERTIES), propMap);
  }

  public void setProperties(String namespace,
                            Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(namespace, getMultipartKey(namespace), propMap);
  }

  public void setProperties(String namespace, String appId, Map<String, String> propMap)
    throws NotFoundException, ProfileConflictException {
    setConfig(namespace, getMultipartKey(namespace, appId), propMap);
  }

  public void setProperties(String namespace, String appId, String programType, String programId,
                            Map<String, String> propMap) throws NotFoundException, ProfileConflictException {
    setConfig(namespace, getMultipartKey(namespace, appId, programType, programId), propMap);
  }

  public void deleteProperties() {
    deleteConfig(EMPTY_NAMESPACE, getMultipartKey(INSTANCE_PROPERTIES));
  }

  public void deleteProperties(String namespace) {
    deleteConfig(namespace, getMultipartKey(namespace));
  }

  public void deleteProperties(String namespace, String appId) {
    deleteConfig(namespace, getMultipartKey(namespace, appId));
  }

  public void deleteProperties(String namespace, String appId, String programType, String programId) {
    deleteConfig(namespace, getMultipartKey(namespace, appId, programType, programId));
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
  private void validateAndSetPreferences(String namespace,
                                         Config config) throws NotFoundException, ProfileConflictException {
    Transactionals.execute(transactional, context -> {
      ProfileDataset profileDataset = ProfileDataset.get(context, datasetFramework);
      ConfigDataset configDataset = ConfigDataset.get(context, datasetFramework);
      NamespaceId namespaceId =
        namespace.equals(PreferencesStore.EMPTY_NAMESPACE) ? NamespaceId.SYSTEM : new NamespaceId(namespace);
      validateProfileProperties(namespaceId, config.getProperties(), profileDataset);
      configDataset.createOrUpdate(namespace, PreferencesStore.PREFERENCES_CONFIG_TYPE, config);
    }, NotFoundException.class, ProfileConflictException.class);
  }

  private void validateProfileProperties(NamespaceId namespaceId, Map<String, String> propertyMap,
                                         ProfileDataset profileDataset)
    throws NotFoundException, ProfileConflictException {
    Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(namespaceId, propertyMap);
    if (!profile.isPresent()) {
      return;
    }

    ProfileId profileId = profile.get();
    if (profileDataset.getProfile(profileId).getStatus() == ProfileStatus.DISABLED) {
      throw new ProfileConflictException(String.format("Profile %s in namespace %s is disabled. It cannot be " +
                                                         "assigned to any programs or schedules",
                                                       profileId.getProfile(), profileId.getNamespace()), profileId);
    }
  }
}
