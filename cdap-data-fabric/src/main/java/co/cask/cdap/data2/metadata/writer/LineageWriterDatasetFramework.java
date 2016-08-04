/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetRuntimeContext;
import co.cask.cdap.data2.dataset2.ForwardingDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * {@link DatasetFramework} that also records lineage (program-dataset access) records.
 */
public class LineageWriterDatasetFramework extends ForwardingDatasetFramework implements ProgramContextAware {

  private static final Logger LOG = LoggerFactory.getLogger(LineageWriterDatasetFramework.class);
  private static final AuthorizationEnforcer SYSTEM_NAMESPACE_ENFORCER = new NoOpAuthorizer();
  private static final DefaultDatasetRuntimeContext.DatasetAccessRecorder SYSTEM_NAMESPACE_ACCESS_RECORDER =
    new DefaultDatasetRuntimeContext.DatasetAccessRecorder() {
      @Override
      public void recordLineage(AccessType accessType) {
        // no-op
      }

      @Override
      public void emitAudit(AccessType accessType) {
        // no-op
      }
    };

  private final RuntimeUsageRegistry runtimeUsageRegistry;
  private final LineageWriter lineageWriter;
  private final AuthenticationContext authenticationContext;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final ProgramContext programContext;

  private AuditPublisher auditPublisher;

  @Inject
  public LineageWriterDatasetFramework(@Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                                       LineageWriter lineageWriter,
                                       RuntimeUsageRegistry runtimeUsageRegistry,
                                       AuthenticationContext authenticationContext,
                                       AuthorizationEnforcer authorizationEnforcer) {
    super(datasetFramework);
    this.lineageWriter = lineageWriter;
    this.runtimeUsageRegistry = runtimeUsageRegistry;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
    this.programContext = new ProgramContext();
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void initContext(Id.Run run) {
    programContext.initContext(run);
  }

  @Override
  public void initContext(Id.Run run, Id.NamespacedId componentId) {
    programContext.initContext(run, componentId);
  }

  @Override
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.addInstance(datasetTypeName, datasetInstanceId, props);

  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.updateInstance(datasetInstanceId, props);
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    delegate.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    delegate.deleteAllInstances(namespaceId);
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(final Id.DatasetInstance datasetInstanceId,
                                          @Nullable final Map<String, String> arguments,
                                          @Nullable final ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return getDataset(datasetInstanceId, arguments, classLoader,
                      new ConstantClassLoaderProvider(classLoader), null, AccessType.UNKNOWN);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(final Id.DatasetInstance datasetInstanceId,
                                          @Nullable final Map<String, String> arguments,
                                          @Nullable final ClassLoader classLoader,
                                          final DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable final Iterable<? extends Id> owners,
                                          final AccessType accessType)
    throws DatasetManagementException, IOException {
    Principal principal = authenticationContext.getPrincipal();
    try {
      // For system, skip authorization and lineage (user program shouldn't allow to access system dataset CDAP-6649)
      // For non-system dataset, always perform authorization and lineage.
      AuthorizationEnforcer enforcer;
      DefaultDatasetRuntimeContext.DatasetAccessRecorder accessRecorder;
      if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
        enforcer = SYSTEM_NAMESPACE_ENFORCER;
        accessRecorder = SYSTEM_NAMESPACE_ACCESS_RECORDER;
      } else {
        enforcer = authorizationEnforcer;
        accessRecorder = new BasicDatasetAccessRecorder(datasetInstanceId, accessType, owners);
      }

      return DefaultDatasetRuntimeContext.execute(enforcer, accessRecorder, principal,
                                                  datasetInstanceId.toEntityId(), new Callable<T>() {
          @Override
          public T call() throws Exception {
            return LineageWriterDatasetFramework.super.getDataset(datasetInstanceId, arguments, classLoader,
                                                                  classLoaderProvider, owners, accessType);
          }
        });
    } catch (IOException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to create dataset instance: " + datasetInstanceId, e);
    }
  }

  @Override
  public void writeLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    super.writeLineage(datasetInstanceId, accessType);
    publishAudit(datasetInstanceId, accessType);
    doWriteLineage(datasetInstanceId, accessType);
  }

  private void doWriteLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    Id.Run programRunId = programContext.getRun();
    if (programRunId != null) {
      Id.NamespacedId componentId = programContext.getComponentId();
      try {
        lineageWriter.addAccess(programRunId, datasetInstanceId, accessType, componentId);
      } catch (Throwable t) {
        // Failure to write to lineage shouldn't cause dataset operation failure
        LOG.warn("Failed to write lineage information for dataset {} with access type {} from {},{}",
                 datasetInstanceId, accessType, programRunId, componentId);
        // Log the stacktrace as debug to not polluting the log
        LOG.debug("Cause for lineage writing failure for {} {} {} {}",
                  datasetInstanceId, accessType, programRunId, componentId, t);
      }
    }
  }

  private void publishAudit(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    Id.Run programRunId = programContext.getRun();
    if (programRunId != null) {
      try {
        AuditPublishers.publishAccess(auditPublisher, datasetInstanceId, accessType, programRunId);
      } catch (Throwable t) {
        // TODO: CDAP-5244. Ideally we should fail if failed to publish audit.
        LOG.warn("Failed to write audit information for dataset {} with access type {} from {}",
                 datasetInstanceId, accessType, programRunId);
        // Log the stacktrace as debug to not polluting the log
        LOG.debug("Cause for audit writing failure for {} {} {}",
                  datasetInstanceId, accessType, programRunId, t);
      }
    }
  }

  private final class BasicDatasetAccessRecorder implements DefaultDatasetRuntimeContext.DatasetAccessRecorder {

    private final AccessType requestedAccessType;
    private final Id.DatasetInstance datasetInstanceId;

    @Nullable
    private final Iterable<? extends Id> owners;

    private BasicDatasetAccessRecorder(Id.DatasetInstance datasetInstanceId, AccessType accessType,
                                       @Nullable Iterable<? extends Id> owners) {
      this.datasetInstanceId = datasetInstanceId;
      this.requestedAccessType = accessType;
      this.owners = owners;
    }

    @Override
    public void recordLineage(AccessType accessType) {
      // If the access type is unknown, default it to the access type being provided to the getDataset call
      if (accessType == AccessType.UNKNOWN) {
        accessType = requestedAccessType;
      }
      writeLineage(datasetInstanceId, accessType);
      if (null == owners) {
        return;
      }
      try {
        runtimeUsageRegistry.registerAll(owners, datasetInstanceId);
      } catch (Exception e) {
        LOG.warn("Failed to register usage of {} -> {}", owners, datasetInstanceId, e);
      }
    }

    @Override
    public void emitAudit(AccessType accessType) {
      // If the access type is unknown, default it to the access type being provided to the getDataset call
      if (accessType == AccessType.UNKNOWN) {
        accessType = requestedAccessType;
      }
      publishAudit(datasetInstanceId, accessType);
    }
  }
}
