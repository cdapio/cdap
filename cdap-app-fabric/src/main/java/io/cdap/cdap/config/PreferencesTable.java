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

package io.cdap.cdap.config;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ParentedId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final StructuredTable table;

  public PreferencesTable(StructuredTableContext context) {
    this.table = context.getTable(StoreDefinition.PreferencesStore.PREFERENCES);
  }

  /**
   * Get the preferences for the entity id.
   *
   * @param entityId the entity id to get the preferences from
   * @return the map which contains the preferences
   */
  public PreferencesDetail getPreferences(EntityId entityId) throws IOException {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        return get(EMPTY_NAMESPACE, INSTANCE_PREFERENCE, entityId.getEntityName());
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        return get(namespaceId.getNamespace(), NAMESPACE_PREFERENCE, namespaceId.getNamespace());
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        return get(appId.getNamespace(), APPLICATION_PREFERENCE, appId.getApplication());
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        return get(programId.getNamespace(), PROGRAM_PREFERENCE, getProgramName(programId));
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  /**
   * Verify that the preferences for the entity id were written with a greater or equal sequence id.
   *
   * @param entityId the entity id to verify
   * @param afterId  the sequence id to check
   * @throws ConflictException if the latest version of the preferences is older than the given sequence id
   */
  public void ensureSequence(EntityId entityId, long afterId)
    throws IOException, ConflictException {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        checkSeqId(EMPTY_NAMESPACE, INSTANCE_PREFERENCE, entityId.getEntityName(), afterId);
        return;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        checkSeqId(namespaceId.getNamespace(), NAMESPACE_PREFERENCE, namespaceId.getNamespace(), afterId);
        return;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        checkSeqId(appId.getNamespace(), APPLICATION_PREFERENCE, appId.getApplication(), afterId);
        return;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        checkSeqId(programId.getNamespace(), PROGRAM_PREFERENCE, getProgramName(programId), afterId);
        return;
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  /**
   * Get the resolved preferences for the entity id, the preferences are resolved from
   * instance -> namespace -> application -> program level
   * (i.e. preferences at child level e.g. program take precedence over those at parent level e.g. application)
   *
   * @param entityId the entity id to get the preferences from
   * @return preferences detail
   */
  public PreferencesDetail getResolvedPreferences(EntityId entityId) throws IOException {
    // if it is instance level get the properties and return
    if (entityId.getEntityType().equals(EntityType.INSTANCE)) {
      PreferencesDetail preferences = getPreferences(entityId);
      return new PreferencesDetail(preferences.getProperties(), preferences.getSeqId(), true);
    }

    PreferencesDetail parentPreferences = null;
    // if the entity id has a parent id, get the preference from its parent
    if (entityId instanceof ParentedId) {
      parentPreferences = getResolvedPreferences(((ParentedId) entityId).getParent());
    } else if (entityId.getEntityType() == EntityType.NAMESPACE) {
      parentPreferences = getResolvedPreferences(new InstanceId(""));
    }
    PreferencesDetail preferences = getPreferences(entityId);

    PreferencesDetail resolved = new PreferencesDetail(preferences.getProperties(), preferences.getSeqId(),
                                                       true);
    if (parentPreferences != null) {
      resolved = PreferencesDetail.resolve(parentPreferences, resolved);
    }
    return resolved;
  }

  /**
   * Get a single resolved preference for the entity id, the preferences are resolved from instance -> namespace -> app
   * -> program level.
   *
   * @param entityId the entity id to get the preferences from
   * @param name     the name of the preference to resolve
   * @return the resolved value of the preference, or null of the named preference is not there
   */
  @Nullable
  public String getResolvedPreference(EntityId entityId, String name) throws IOException {
    // get the preference for the entity itself
    String value = getPreferences(entityId).getProperties().get(name);
    if (value != null) {
      return value;
    }
    // if the value is null and the entity has a parent, defer to the parent
    if (entityId instanceof ParentedId) {
      value = getResolvedPreference(((ParentedId) entityId).getParent(), name);
    } else {
      // if there is no parent get from the instance id
      value = getPreferences(new InstanceId("")).getProperties().get(name);
    }
    return value;
  }

  /**
   * Set the preferences for the entity id.
   *
   * @param entityId the entity id to set the preferences from
   * @param propMap  the map which contains the preferences
   * @return the sequence id of the operation
   */
  public long setPreferences(EntityId entityId, Map<String, String> propMap) throws IOException {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        return upsert(
          EMPTY_NAMESPACE, INSTANCE_PREFERENCE, new Config(entityId.getEntityName(), propMap));
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        return upsert(
          namespaceId.getNamespace(), NAMESPACE_PREFERENCE, new Config(namespaceId.getNamespace(), propMap));
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        return upsert(
          appId.getNamespace(), APPLICATION_PREFERENCE, new Config(appId.getApplication(), propMap));
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        return upsert(
          programId.getNamespace(), PROGRAM_PREFERENCE, new Config(getProgramName(programId), propMap));
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  /**
   * Delete the preferences for the entity id.
   *
   * @param entityId the entity id to delete the preferences
   * @return the sequence id of the operation, or -1 if no preferences existed for the entity id
   */
  public long deleteProperties(EntityId entityId) throws IOException {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        return delete(EMPTY_NAMESPACE, INSTANCE_PREFERENCE, entityId.getEntityName());
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        return delete(namespaceId.getNamespace(), NAMESPACE_PREFERENCE, namespaceId.getNamespace());
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        return delete(appId.getNamespace(), APPLICATION_PREFERENCE, appId.getApplication());
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        return delete(programId.getNamespace(), PROGRAM_PREFERENCE, getProgramName(programId));
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
  }

  /**
   * Used to dedup programs in different applications or types.
   */
  private String getProgramName(ProgramId programId) {
    return String.join(",", programId.getApplication(), programId.getType().getCategoryName(),
                       programId.getProgram());
  }

  private long upsert(String namespace, String type, Config config) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, config.getName());
    Optional<StructuredRow> row = table.read(primaryKey);
    long seq = 1;
    if (row.isPresent()) {
      Long currentSeq = row.get().getLong(StoreDefinition.PreferencesStore.SEQUENCE_ID_FIELD);
      if (currentSeq != null) {
        seq = currentSeq + 1;
      }
    }
    table.upsert(toFields(namespace, type, config, seq));
    return seq;
  }

  private long delete(String namespace, String type, String name) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
    Optional<StructuredRow> row = table.read(primaryKey);
    if (!row.isPresent() || row.get().getString(StoreDefinition.PreferencesStore.PROPERTIES_FIELD) == null) {
      return -1;
    }
    Long currentSeq = row.get().getLong(StoreDefinition.PreferencesStore.SEQUENCE_ID_FIELD);
    long seq = currentSeq == null ? 1L : currentSeq + 1;
    table.upsert(toFields(namespace, type, name, seq));
    return seq;
  }

  private void checkSeqId(String namespace, String type, String name, long seq)
    throws IOException, ConflictException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
    Optional<StructuredRow> row = table.read(primaryKey);
    Long currentSeq = null;
    if (row.isPresent()) {
      currentSeq = row.get().getLong(StoreDefinition.PreferencesStore.SEQUENCE_ID_FIELD);
      if (currentSeq != null && currentSeq >= seq) {
        return;
      }
    }
    throw new ConflictException(String.format("Expected sequence id >= %d for %s %s in namespace %s, but found %s",
                                              seq, type, name, namespace, String.valueOf(currentSeq)));
  }

  private PreferencesDetail get(String namespace, String type, String name) throws IOException {
    List<Field<?>> primaryKey = getPrimaryKey(namespace, type, name);
    Optional<StructuredRow> row = table.read(primaryKey);
    Map<String, String> properties = Collections.emptyMap();
    Long seqId = new Long(0);
    if (row.isPresent()) {
      String string = row.get().getString(StoreDefinition.PreferencesStore.PROPERTIES_FIELD);
      seqId = row.get().getLong(StoreDefinition.PreferencesStore.SEQUENCE_ID_FIELD);
      if (string != null) {
        properties = GSON.fromJson(string, MAP_TYPE);
      }
    }
    return new PreferencesDetail(properties, seqId, false);
  }

  private List<Field<?>> toFields(String namespace, String type, Config config, long seqId) {
    List<Field<?>> fields = getPrimaryKey(namespace, type, config.getName());
    fields.add(Fields.stringField(StoreDefinition.PreferencesStore.PROPERTIES_FIELD,
                                  GSON.toJson(config.getProperties())));
    fields.add(Fields.longField(StoreDefinition.PreferencesStore.SEQUENCE_ID_FIELD, seqId));
    return fields;
  }

  private List<Field<?>> toFields(String namespace, String type, String configName, long seqId) {
    List<Field<?>> fields = getPrimaryKey(namespace, type, configName);
    fields.add(Fields.stringField(StoreDefinition.PreferencesStore.PROPERTIES_FIELD, null));
    fields.add(Fields.longField(StoreDefinition.PreferencesStore.SEQUENCE_ID_FIELD, seqId));
    return fields;
  }

  private List<Field<?>> getPrimaryKey(String namespace, String type, String name) {
    List<Field<?>> primaryKey = new ArrayList<>();
    primaryKey.add(Fields.stringField(StoreDefinition.PreferencesStore.NAMESPACE_FIELD, namespace));
    primaryKey.add(Fields.stringField(StoreDefinition.PreferencesStore.TYPE_FIELD, type));
    primaryKey.add(Fields.stringField(StoreDefinition.PreferencesStore.NAME_FIELD, name));
    return primaryKey;
  }
}
