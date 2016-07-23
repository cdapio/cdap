/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * An {@link AuthorizationContext} that delegates to the provided {@link DatasetContext}, {@link Admin} and
 * {@link Transactional}.
 */
public class DefaultAuthorizationContext implements AuthorizationContext {
  private final Properties extensionProperties;
  private final DatasetContext delegateDatasetContext;
  private final Admin delegateAdmin;
  private final Transactional delegateTxnl;
  private final AuthenticationContext delegateAuthenticationContext;

  @Inject
  @VisibleForTesting
  public DefaultAuthorizationContext(@Assisted("extension-properties") Properties extensionProperties,
                                     DatasetContext delegateDatasetContext, Admin delegateAdmin,
                                     Transactional delegateTxnl, AuthenticationContext delegateAuthenticationContext) {
    this.extensionProperties = extensionProperties;
    this.delegateDatasetContext = delegateDatasetContext;
    this.delegateAdmin = delegateAdmin;
    this.delegateTxnl = delegateTxnl;
    this.delegateAuthenticationContext = delegateAuthenticationContext;
  }

  @Override
  public boolean datasetExists(String name) throws DatasetManagementException {
    return delegateAdmin.datasetExists(name);
  }

  @Override
  public String getDatasetType(String name) throws DatasetManagementException {
    return delegateAdmin.getDatasetType(name);
  }

  @Override
  public DatasetProperties getDatasetProperties(String name) throws DatasetManagementException {
    return delegateAdmin.getDatasetProperties(name);
  }

  @Override
  public void createDataset(String name, String type, DatasetProperties properties) throws DatasetManagementException {
    delegateAdmin.createDataset(name, type, properties);
  }

  @Override
  public void updateDataset(String name, DatasetProperties properties) throws DatasetManagementException {
    delegateAdmin.updateDataset(name, properties);
  }

  @Override
  public void dropDataset(String name) throws DatasetManagementException {
    delegateAdmin.dropDataset(name);
  }

  @Override
  public void truncateDataset(String name) throws DatasetManagementException {
    delegateAdmin.truncateDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return delegateDatasetContext.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    return delegateDatasetContext.getDataset(namespace, name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return delegateDatasetContext.getDataset(name, arguments);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return delegateDatasetContext.getDataset(namespace, name, arguments);
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    delegateDatasetContext.releaseDataset(dataset);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    delegateDatasetContext.discardDataset(dataset);
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    delegateTxnl.execute(runnable);
  }

  @Override
  public Properties getExtensionProperties() {
    return extensionProperties;
  }

  @Override
  public void putSecureData(String namespace, String name, byte[] data, String description,
                            Map<String, String> properties) throws IOException {
    delegateAdmin.putSecureData(namespace, name, data, description, properties);
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws IOException {
    delegateAdmin.deleteSecureData(namespace, name);
  }

  public Principal getPrincipal() {
    return delegateAuthenticationContext.getPrincipal();
  }
}
