/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Creates dataset instances.
 */
final class DatasetInstanceCreator {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceCreator.class);
  private final DatasetFramework datasetFramework;
  private final boolean allowDatasetUncheckedUpgrade;

  DatasetInstanceCreator(CConfiguration configuration, DatasetFramework datasetFramework) {
    this.datasetFramework = datasetFramework;
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param namespaceId the namespace to create the dataset instance in
   * @param datasets the datasets to create
   * @param ownerPrincipal the principal of the owner for the datasets to be created.
   * @param authorizingUser the authorizing user who will be making the call
   */
  void createInstances(NamespaceId namespaceId, Map<String, DatasetCreationSpec> datasets,
                       @Nullable final KerberosPrincipalId ownerPrincipal,
                       String authorizingUser) throws Exception {
    // create dataset instances
    for (Map.Entry<String, DatasetCreationSpec> instanceEntry : datasets.entrySet()) {
      String instanceName = instanceEntry.getKey();
      final DatasetId instanceId = namespaceId.dataset(instanceName);
      final DatasetCreationSpec instanceSpec = instanceEntry.getValue();
      DatasetSpecification existingSpec =
        AuthorizationUtil.authorizeAs(authorizingUser, new Callable<DatasetSpecification>() {
          @Override
          public DatasetSpecification call() throws Exception {
            return datasetFramework.getDatasetSpec(instanceId);
          }
        });
      if (existingSpec == null) {
        LOG.info("Adding dataset instance: {}", instanceName);
        AuthorizationUtil.authorizeAs(authorizingUser, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId, instanceSpec.getProperties(),
                                         ownerPrincipal);
            return null;
          }
        });
      } else {
        if (!existingSpec.getType().equals(instanceSpec.getTypeName())) {
          throw new IncompatibleUpdateException(
            String.format("Existing dataset '%s' of type '%s' may not be updated to type '%s'",
                          instanceName, existingSpec.getType(), instanceSpec.getTypeName()));
        }
        if (allowDatasetUncheckedUpgrade) {
          LOG.info("Updating dataset instance: {}", instanceName);
          AuthorizationUtil.authorizeAs(authorizingUser, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              datasetFramework.updateInstance(instanceId, instanceSpec.getProperties());
              return null;
            }
          });
        }
      }
    }
  }
}
