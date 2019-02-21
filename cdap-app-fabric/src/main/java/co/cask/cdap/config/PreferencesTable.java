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

package co.cask.cdap.config;

import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.spi.data.StructuredTableContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class is responsible for preferences operations.
 * It does not wrap its operations in a transaction. It is up to the caller to decide what operations belong
 * in a transaction.
 */
public class PreferencesTable {
  private static final String EMPTY_NAMESPACE = "";
  private static final String INSTANCE_PREFERENCE = "instance_preference";
  private static final String NAMESPACE_PREFERENCE = "namespace_preference";
  private static final String APPLICATION_PREFERENCE = "application_preference";
  private static final String PROGRAM_PREFERENCE = "program_preference";

  private ConfigTable table;

  public PreferencesTable(StructuredTableContext context) {
    this.table = new ConfigTable(context);
  }

  /**
   * Get the preferences for the entity id
   *
   * @param entityId the entity id to get the preferences from
   * @return the map which contains the preferences
   */
  public Map<String, String> getPreferences(EntityId entityId) throws IOException {
    try {
      switch (entityId.getEntityType()) {
        case INSTANCE:
          return table.get(EMPTY_NAMESPACE, INSTANCE_PREFERENCE, entityId.getEntityName()).getProperties();
        case NAMESPACE:
          NamespaceId namespaceId = (NamespaceId) entityId;
          return table.get(namespaceId.getNamespace(), NAMESPACE_PREFERENCE, namespaceId.getNamespace())
            .getProperties();
        case APPLICATION:
          ApplicationId appId = (ApplicationId) entityId;
          return table.get(appId.getNamespace(), APPLICATION_PREFERENCE, appId.getApplication()).getProperties();
        case PROGRAM:
          ProgramId programId = (ProgramId) entityId;
          return table.get(programId.getNamespace(), PROGRAM_PREFERENCE, getProgramName(programId)).getProperties();
        default:
          throw new UnsupportedOperationException(
            String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
      }
    } catch (ConfigNotFoundException e) {
      // no-op - return empty map
      return new HashMap<>();
    }
  }

  /**
   * Get the resolved preferences for the entity id, the preferences are resolved from instance -> namespace -> app
   * -> program level
   *
   * @param entityId the entity id to get the preferences from
   * @return the map which contains the preferences
   */
  public Map<String, String> getResolvedPreferences(EntityId entityId) throws IOException {
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
   * Get a single resolved preference for the entity id, the preferences are resolved from instance -> namespace -> app
   * -> program level
   *
   * @param entityId the entity id to get the preferences from
   * @param name the name of the preference to resolve
   * @return the resolved value of the preference, or null of the named preference is not there
   */
  @Nullable
  public String getResolvedPreference(EntityId entityId, String name) throws IOException {
    // get the preference for the entity itself
    String value = getPreferences(entityId).get(name);
    // if the value is null and the entity has a parent, defer to the parent
    if (value == null && entityId instanceof ParentedId) {
      value = getResolvedPreference(((ParentedId) entityId).getParent(), name);
    }
    // return whatever we have
    return value;
  }

  /**
   * Set the preferences for the entity id
   *
   * @param entityId the entity id to set the preferences from
   * @param propMap the map which contains the preferences
   */
  public void setPreferences(EntityId entityId, Map<String, String> propMap) throws IOException {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        table.createOrUpdate(EMPTY_NAMESPACE, INSTANCE_PREFERENCE, new Config(entityId.getEntityName(), propMap));
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        table.createOrUpdate(
          namespaceId.getNamespace(), NAMESPACE_PREFERENCE, new Config(namespaceId.getNamespace(), propMap));
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        table.createOrUpdate(
          appId.getNamespace(), APPLICATION_PREFERENCE, new Config(appId.getApplication(), propMap));
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        table.createOrUpdate(
          programId.getNamespace(), PROGRAM_PREFERENCE, new Config(getProgramName(programId), propMap));
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
  public void deleteProperties(EntityId entityId) throws IOException {
    try {
      switch (entityId.getEntityType()) {
        case INSTANCE:
          table.delete(EMPTY_NAMESPACE, INSTANCE_PREFERENCE, entityId.getEntityName());
          break;
        case NAMESPACE:
          NamespaceId namespaceId = (NamespaceId) entityId;
          table.delete(namespaceId.getNamespace(), NAMESPACE_PREFERENCE, namespaceId.getNamespace());
          break;
        case APPLICATION:
          ApplicationId appId = (ApplicationId) entityId;
          table.delete(appId.getNamespace(), APPLICATION_PREFERENCE, appId.getApplication());
          break;
        case PROGRAM:
          ProgramId programId = (ProgramId) entityId;
          table.delete(programId.getNamespace(), PROGRAM_PREFERENCE, getProgramName(programId));
          break;
        default:
          throw new UnsupportedOperationException(
            String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
      }
    } catch (ConfigNotFoundException e) {
      //no-op
    }
  }

  /**
   * Used to dedup programs in different applications or types.
   */
  private String getProgramName(ProgramId programId) {
    return String.join(",", programId.getApplication(), programId.getType().getCategoryName(),
                       programId.getProgram());
  }
}
