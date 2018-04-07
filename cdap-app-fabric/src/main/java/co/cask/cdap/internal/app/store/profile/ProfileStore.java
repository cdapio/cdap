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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Store for profile.
 */
public class ProfileStore {
  private static final DatasetId DATASET_ID = NamespaceId.SYSTEM.dataset("app.meta");
  private static final String PROFILE_PREFIX = "profile";
  private static final DatasetProperties TABLE_PROPERTIES =
    TableProperties.builder().setConflictDetection(ConflictDetection.COLUMN).build();

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public ProfileStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   txClient, DATASET_ID.getParent(),
                                                                   Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by profile store.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), DATASET_ID, TABLE_PROPERTIES);
  }

  /**
   * Get the profile information about the given profile
   *
   * @param profileId the id of the profile to look up
   * @return the profile information about the given profile
   * @throws IOException if there was an IO error looking up the profile
   * @throws NotFoundException if the profile is not found
   */
  public Profile getProfile(ProfileId profileId) throws IOException, NotFoundException {
    return Transactionals.execute(transactional, context -> {
      Profile profile = getMDS(context).get(getRowKey(profileId), Profile.class);
      if (profile == null) {
        throw new NotFoundException(profileId);
      }
      return profile;
    }, IOException.class, NotFoundException.class);
  }

  /**
   * Get the profile information in the given namespace
   *
   * @param namespaceId the id of the profile to look up
   * @return the list of profiles which is in this namespace
   * @throws IOException if there was an IO error looking up the profile
   */
  public List<Profile> getProfiles(NamespaceId namespaceId) throws IOException {
    return Transactionals.execute(transactional, context -> {
      List<Profile> profiles = getMDS(context).list(getRowKey(namespaceId), Profile.class);
      return Collections.unmodifiableList(profiles);
    }, IOException.class);
  }

  /**
   * Add the profile to the profile store.
   *
   * @param profileId the id of the profile to add
   * @param profile the information of the profile
   * @throws IOException if there was an IO error adding the profile
   * @throws AlreadyExistsException if the profile already exists
   */
  public void add(ProfileId profileId, Profile profile) throws IOException, AlreadyExistsException {
    Transactionals.execute(transactional, context -> {
      MetadataStoreDataset mds = getMDS(context);
      // make sure that a profile doesn't exist
      MDSKey rowKey = getRowKey(profileId);
      Profile value = mds.get(rowKey, Profile.class);
      if (value != null) {
        throw new AlreadyExistsException(profileId,
                                         String.format("Profile '%s' already exists.",
                                                       profile.getName()));
      }
      mds.write(rowKey, profile);
    }, IOException.class, AlreadyExistsException.class);
  }

  /**
   * Deletes the profile from the profile store
   *
   * @param profileId the id of the profile to delete
   * @throws IOException if there was an IO error deleting the profile
   * @throws NotFoundException if the profile is not found
   */
  public void delete(ProfileId profileId) throws IOException, NotFoundException {
    Transactionals.execute(transactional, context -> {
      MetadataStoreDataset mds = getMDS(context);
      MDSKey rowKey = getRowKey(profileId);
      Profile value = getMDS(context).get(rowKey, Profile.class);
      if (value == null) {
        throw new NotFoundException(profileId);
      }
      mds.delete(getRowKey(profileId));
    }, IOException.class, NotFoundException.class);
  }

  /**
   * Delete all profiles in a given namespace.
   *
   * @param namespaceId the id of the namespace
   * @throws IOException if there was an IO error deleting the profiles
   */
  public void deleteAll(NamespaceId namespaceId) throws IOException {
    Transactionals.execute(transactional, context -> {
      getMDS(context).deleteAll(getRowKey(namespaceId));
    }, IOException.class);
  }

  private MetadataStoreDataset getMDS(DatasetContext context) throws IOException, DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(context, datasetFramework, DATASET_ID, Table.class.getName(),
                                                  TABLE_PROPERTIES);
    return new MetadataStoreDataset(table);
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
