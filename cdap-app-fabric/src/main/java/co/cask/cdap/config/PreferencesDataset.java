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

package co.cask.cdap.config;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.id.ProgramId;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class is responsible for Perferences operations. It uses {@link ConfigDataset} as the underlying storage.
 * It does not wrap its operations in a transaction. It is up to the caller to decide what operations belong
 * in a transaction.
 */
public class PreferencesDataset {
  private static final String PREFERENCES_CONFIG_TYPE = "preferences";
  // Namespace where instance level properties are stored
  private static final String EMPTY_NAMESPACE = "";
  // Id for Properties config stored at the instance level
  private static final String INSTANCE_PROPERTIES = "instance";

  private final ConfigDataset configDataset;

  private PreferencesDataset(ConfigDataset configDataset) {
    this.configDataset = configDataset;
  }

  /**
   * Get the PreferenceDataset
   */
  public static PreferencesDataset get(DatasetContext datasetContext, DatasetFramework dsFramework) {
    return new PreferencesDataset(ConfigDataset.get(datasetContext, dsFramework));
  }

  /**
   * Get the preferences for the entity id
   *
   * @param entityId the entity id to get the preferences from
   * @return the map which contains the preferences
   */
  public Map<String, String> getPreferences(EntityId entityId) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        return getConfigProperties(EMPTY_NAMESPACE, getMultipartKey(INSTANCE_PROPERTIES));
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        return getConfigProperties(namespaceId.getNamespace(), getMultipartKey(namespaceId.getNamespace()));
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        return getConfigProperties(appId.getNamespace(), getMultipartKey(appId.getNamespace(), appId.getApplication()));
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        return getConfigProperties(programId.getNamespace(),
                                   getMultipartKey(programId.getNamespace(), programId.getApplication(),
                                                   programId.getType().getCategoryName(), programId.getProgram()));
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  /**
   * Get the resolved preferences for the entity id, the preferences are resolved from instance -> namespace -> app
   * -> program level
   *
   * @param entityId the entity id to get the preferences from
   * @return the map which contains the preferences
   */
  public Map<String, String> getResolvedPreferences(EntityId entityId) {
    // if it is instance level get the properties and return
    if (entityId.getEntityType().equals(EntityType.INSTANCE)) {
      return getPreferences(entityId);
    }

    Map<String, String> properties = new HashMap<>();
    // if the entity id has a parent id, get the preference from its parent
    if (entityId instanceof ParentedId) {
      properties.putAll(getResolvedPreferences(((ParentedId) entityId).getParent()));
    } else if (entityId.getEntityType() == EntityType.NAMESPACE) {
      // otherwise it is a namespace id, which we want to look at the instance level
      properties.putAll(getResolvedPreferences(new InstanceId("")));
    }
    // put the current level property
    properties.putAll(getPreferences(entityId));
    return properties;
  }

  /**
   * Set the preferences for the entity id
   *
   * @param entityId the entity id to set the preferences from
   * @param propMap the map which contains the preferences
   */
  public void setPreferences(EntityId entityId, Map<String, String> propMap) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        setConfig(EMPTY_NAMESPACE, getMultipartKey(INSTANCE_PROPERTIES), propMap);
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        setConfig(namespaceId.getNamespace(), getMultipartKey(namespaceId.getNamespace()), propMap);
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        setConfig(appId.getNamespace(), getMultipartKey(appId.getNamespace(), appId.getApplication()), propMap);
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        setConfig(programId.getNamespace(),
                  getMultipartKey(programId.getNamespace(), programId.getApplication(),
                                  programId.getType().getCategoryName(), programId.getProgram()), propMap);
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  /**
   * Delete the preferences for the entity id
   *
   * @param entityId the entity id to delete the preferences
   */
  public void deleteProperties(EntityId entityId) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        deleteConfig(EMPTY_NAMESPACE, getMultipartKey(INSTANCE_PROPERTIES));
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        deleteConfig(namespaceId.getNamespace(), getMultipartKey(namespaceId.getNamespace()));
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        deleteConfig(appId.getNamespace(), getMultipartKey(appId.getNamespace(), appId.getApplication()));
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        deleteConfig(programId.getNamespace(),
                     getMultipartKey(programId.getNamespace(), programId.getApplication(),
                                     programId.getType().getCategoryName(), programId.getProgram()));
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  private Map<String, String> getConfigProperties(String namespace, String id) {
    Map<String, String> value = new HashMap<>();
    try {
      Config config = configDataset.get(namespace, PREFERENCES_CONFIG_TYPE, id);
      value.putAll(config.getProperties());
    } catch (ConfigNotFoundException e) {
      //no-op - return empty map
    }
    return value;
  }

  private void setConfig(String namespace, String id, Map<String, String> propertyMap) {
    Config config = new Config(id, propertyMap);
    configDataset.createOrUpdate(namespace, PREFERENCES_CONFIG_TYPE, config);
  }

  private void deleteConfig(String namespace, String id) {
    try {
      configDataset.delete(namespace, PREFERENCES_CONFIG_TYPE, id);
    } catch (ConfigNotFoundException e) {
      //no-op
    }
  }

  private String getMultipartKey(String... parts) {
    int sizeOfParts = Stream.of(parts).mapToInt(String::length).reduce(0, (a, b) -> a + b);

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
}
