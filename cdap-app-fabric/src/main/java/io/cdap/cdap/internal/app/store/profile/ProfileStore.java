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

package io.cdap.cdap.internal.app.store.profile;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Store for profile information, and profile assignments to entities.
 * This dataset does not wrap its operations in a transaction. It is up to the caller
 * to decide what operations belong in a transaction.
 *
 * The table will look like:
 * primary key                                                 column -> value
 * [namespace][profile-id]                                     profile_data -> Profile{@link Profile}
 * [namespace][profile-id][entity-id]                          entity_data -> EntityId{@link EntityId}
 */
public class ProfileStore {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter()).create();

  private final StructuredTable profileTable;
  private final StructuredTable profileEntityTable;

  public ProfileStore(StructuredTable profileTable, StructuredTable profileEntityTable) {
    this.profileTable = profileTable;
    this.profileEntityTable = profileEntityTable;
  }

  public static ProfileStore get(StructuredTableContext context) {
    try {
      return new ProfileStore(
        context.getTable(StoreDefinition.ProfileStore.PROFILE_STORE_TABLE),
        context.getTable(StoreDefinition.ProfileStore.PROFILE_ENTITY_STORE_TABLE)
      );
    } catch (TableNotFoundException e) {
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
  public Profile getProfile(ProfileId profileId) throws NotFoundException, IOException {
    Profile profile = getProfileInternal(getProfileKeys(profileId));
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
  public List<Profile> getProfiles(NamespaceId namespaceId, boolean includeSystem) throws IOException {
    List<Profile> profiles = new ArrayList<>();
    scanProfiles(namespaceId, profiles);
    if (includeSystem && !namespaceId.equals(NamespaceId.SYSTEM)) {
      scanProfiles(NamespaceId.SYSTEM, profiles);
    }
    return Collections.unmodifiableList(profiles);
  }

  /**
   * Save the profile to the profile store. By default the profile status will be enabled.
   *
   * @param profileId the id of the profile to save
   * @param profile the information of the profile
   */
  public void saveProfile(ProfileId profileId, Profile profile) throws IOException {
    Collection<Field<?>> fields = getProfileKeys(profileId);
    Profile oldProfile = getProfileInternal(fields);
    Profile newProfile =
      new Profile(profile.getName(), profile.getLabel(), profile.getDescription(), profile.getScope(),
                  oldProfile == null ? ProfileStatus.ENABLED : oldProfile.getStatus(),
                  profile.getProvisioner(),
                  oldProfile == null ? profile.getCreatedTsSeconds() : oldProfile.getCreatedTsSeconds());

    fields.add(Fields.stringField(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD, GSON.toJson(newProfile)));
    profileTable.upsert(fields);
  }

  /**
   * Add a profile if it does not exist in the store
   *
   * @param profileId the id of the profile to add
   * @param profile the information of the profile
   */
  public void createIfNotExists(ProfileId profileId, Profile profile) throws IOException {
    Collection<Field<?>> keys = getProfileKeys(profileId);
    Profile newProfile = new Profile(profile.getName(), profile.getLabel(), profile.getDescription(),
                                     profile.getScope(), ProfileStatus.ENABLED, profile.getProvisioner());
    profileTable.compareAndSwap(
      keys,
      Fields.stringField(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD, null),
      Fields.stringField(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD, GSON.toJson(newProfile))
    );
  }

  /**
   * Deletes the profile from the profile store
   *
   * @param profileId the id of the profile to delete
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is enabled
   */
  public void deleteProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException, IOException {
    Collection<Field<?>> fields = getProfileKeys(profileId);
    Profile value = getProfileInternal(fields);
    if (value == null) {
      throw new NotFoundException(profileId);
    }
    if (value.getStatus() == ProfileStatus.ENABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s in namespace %s is currently enabled. A profile can " +
                        "only be deleted if it is disabled", profileId.getProfile(), profileId.getNamespace()),
        profileId);
    }
    profileTable.delete(getProfileKeys(profileId));
  }

  /**
   * Delete all profiles in a given namespace.
   */
  @VisibleForTesting
  public void deleteAllProfiles() throws IOException {
    profileTable.deleteAll(Range.all());
    profileEntityTable.deleteAll(Range.all());
  }

  /**
   * Enable the profile. After the profile is enabled, any program/schedule can be associated with this profile.
   *
   * @param profileId the id of the profile to enable
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is already enabled
   */
  public void enableProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException, IOException {
    changeProfileStatus(profileId, ProfileStatus.ENABLED);
  }

  /**
   * Disable the profile. After the profile is disabled, any program/schedule cannot be associated with this profile.
   *
   * @param profileId the id of the profile to disable
   * @throws NotFoundException if the profile is not found
   * @throws ProfileConflictException if the profile is already disabled
   */
  public void disableProfile(ProfileId profileId) throws NotFoundException, ProfileConflictException, IOException {
    changeProfileStatus(profileId, ProfileStatus.DISABLED);
  }

  private void changeProfileStatus(ProfileId profileId, ProfileStatus expectedStatus)
    throws NotFoundException, ProfileConflictException, IOException {
    Collection<Field<?>> fields = getProfileKeys(profileId);
    Profile oldProfile = getProfileInternal(fields);
    if (oldProfile == null) {
      throw new NotFoundException(profileId);
    }
    if (oldProfile.getStatus() == expectedStatus) {
      throw new ProfileConflictException(
        String.format("Profile %s already %s", profileId.getProfile(), expectedStatus.toString()), profileId);
    }
    Profile newProfile = new Profile(oldProfile.getName(), oldProfile.getLabel(), oldProfile.getDescription(),
                                     oldProfile.getScope(), expectedStatus, oldProfile.getProvisioner(),
                                     oldProfile.getCreatedTsSeconds());
    fields.add(Fields.stringField(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD, GSON.toJson(newProfile)));
    profileTable.upsert(fields);
  }

  /**
   * Get assignments with the profile.
   *
   * @param profileId the profile id
   * @return the set of entities that the profile is assigned to
   * @throws NotFoundException if the profile is not found
   */
  public Set<EntityId> getProfileAssignments(ProfileId profileId) throws NotFoundException, IOException {
    Collection<Field<?>> fields = getProfileKeys(profileId);
    Profile profile = getProfileInternal(fields);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }

    Set<EntityId> entities = new HashSet<>();
    scanEntities(profileId, entities);
    return Collections.unmodifiableSet(entities);
  }

  /**
   * Add an assignment to the profile. Assignment can only be added if the profile is ENABLED
   *
   * @param profileId the profile id
   * @param entityId the entity to add to the assgiment
   */
  public void addProfileAssignment(ProfileId profileId,
                                   EntityId entityId) throws ProfileConflictException, NotFoundException, IOException {
    Collection<Field<?>> fields = getProfileKeys(profileId);
    Profile profile = getProfileInternal(fields);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }
    if (profile.getStatus() == ProfileStatus.DISABLED) {
      throw new ProfileConflictException(
        String.format("Profile %s is DISABLED. No entity can be assigned to it.", profileId.getProfile()), profileId);
    }
    addEntityIdKey(fields, entityId);
    fields.add(Fields.stringField(StoreDefinition.ProfileStore.ENTITY_DATA_FIELD, GSON.toJson(entityId)));
    profileEntityTable.upsert(fields);
  }

  /**
   * Remove an assignment from the profile.
   *
   * @param profileId the profile id
   * @param entityId the entity to remove from the assignment
   * @throws NotFoundException if the profile is not found
   */
  public void removeProfileAssignment(ProfileId profileId, EntityId entityId) throws NotFoundException, IOException {
    Collection<Field<?>> keys = getProfileKeys(profileId);
    Profile profile = getProfileInternal(keys);
    if (profile == null) {
      throw new NotFoundException(profileId);
    }
    addEntityIdKey(keys, entityId);
    profileEntityTable.delete(keys);
  }

  /**
   * Returns names of profiles that have a given property value.
   *
   * @param propertyValue the {@link ProvisionerPropertyValue} to find in profiles
   * @return the list of profile names that have the property value
   */
  public List<String> findProfilesWithProperty(ProvisionerPropertyValue propertyValue) throws IOException {
    try (CloseableIterator<StructuredRow> iterator =
           profileTable.scan(Range.all(), Integer.MAX_VALUE)) {
      List<String> profiles = new ArrayList<>();
      while (iterator.hasNext()) {
        Profile profile = GSON.fromJson(iterator.next()
                                          .getString(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD), Profile.class);
        if (profile.getProvisioner().getProperties().contains(propertyValue)) {
          profiles.add(profile.getName());
        }
      }
      return profiles;
    }
  }

  @SuppressWarnings("OptionalIsPresent")
  @Nullable
  private Profile getProfileInternal(Collection<Field<?>> profileKeys) throws IOException {
    Optional<StructuredRow> rowOptional = profileTable.read(profileKeys);
    if (!rowOptional.isPresent()) {
      return null;
    }
    return GSON.fromJson(rowOptional.get().getString(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD), Profile.class);
  }

  private void scanProfiles(NamespaceId namespaceId, List<Profile> profiles) throws IOException {
    try (CloseableIterator<StructuredRow> iterator =
           profileTable.scan(Range.singleton(getNamespaceKey(namespaceId)), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        profiles.add(GSON.fromJson(iterator.next().getString(StoreDefinition.ProfileStore.PROFILE_DATA_FIELD),
                                   Profile.class));
      }
    }
  }

  private void scanEntities(ProfileId profileId, Collection<EntityId> entities) throws IOException {
    try (CloseableIterator<StructuredRow> iterator =
           profileEntityTable.scan(Range.singleton(getProfileKeys(profileId)), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        entities.add(GSON.fromJson(iterator.next().getString(StoreDefinition.ProfileStore.ENTITY_DATA_FIELD),
                                   EntityId.class));
      }
    }
  }

  private Collection<Field<?>> getProfileKeys(ProfileId profileId) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(StoreDefinition.ProfileStore.NAMESPACE_FIELD, profileId.getNamespace()));
    keys.add(Fields.stringField(StoreDefinition.ProfileStore.PROFILE_ID_FIELD, profileId.getProfile()));
    return keys;
  }

  private Collection<Field<?>> getNamespaceKey(NamespaceId namespaceId) {
    return Collections.singleton(
      Fields.stringField(StoreDefinition.ProfileStore.NAMESPACE_FIELD, namespaceId.getNamespace())
    );
  }

  private void addEntityIdKey(Collection<Field<?>> keys, EntityId entityId) {
    keys.add(Fields.stringField(StoreDefinition.ProfileStore.ENTITY_ID_FIELD, entityId.toString()));
  }
}
