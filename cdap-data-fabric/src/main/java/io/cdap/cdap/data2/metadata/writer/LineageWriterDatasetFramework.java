/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.writer;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.annotation.ReadOnly;
import io.cdap.cdap.api.annotation.ReadWrite;
import io.cdap.cdap.api.annotation.WriteOnly;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.data.ProgramContext;
import io.cdap.cdap.data.ProgramContextAware;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditPublisher;
import io.cdap.cdap.data2.audit.AuditPublishers;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetRuntimeContext;
import io.cdap.cdap.data2.dataset2.ForwardingDatasetFramework;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.registry.UsageWriter;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Map;
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

  private final UsageWriter usageWriter;
  private final LineageWriter lineageWriter;
  private final AuthenticationContext authenticationContext;
  private final AuthorizationEnforcer authorizationEnforcer;
  private volatile ProgramContext programContext;

  private AuditPublisher auditPublisher;

  @Inject
  public LineageWriterDatasetFramework(@Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                                       LineageWriter lineageWriter,
                                       UsageWriter usageWriter,
                                       AuthenticationContext authenticationContext,
                                       AuthorizationEnforcer authorizationEnforcer) {
    super(datasetFramework);
    this.lineageWriter = lineageWriter;
    this.usageWriter = usageWriter;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void setContext(ProgramContext programContext) {
    this.programContext = programContext;
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(final DatasetId datasetInstanceId,
                                          final Map<String, String> arguments,
                                          @Nullable final ClassLoader classLoader,
                                          final DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable final Iterable<? extends EntityId> owners,
                                          final AccessType accessType)
    throws DatasetManagementException, IOException {
    Principal principal = authenticationContext.getPrincipal();
    try {
      // For system, skip authorization and lineage (user program shouldn't allow to access system dataset CDAP-6649)
      // For non-system dataset, always perform authorization and lineage.
      AuthorizationEnforcer enforcer;
      DefaultDatasetRuntimeContext.DatasetAccessRecorder accessRecorder;
      if (!DatasetsUtil.isUserDataset(datasetInstanceId)) {
        enforcer = SYSTEM_NAMESPACE_ENFORCER;
        accessRecorder = SYSTEM_NAMESPACE_ACCESS_RECORDER;
      } else {
        enforcer = authorizationEnforcer;
        accessRecorder = new BasicDatasetAccessRecorder(datasetInstanceId, accessType, owners);
      }

      return DefaultDatasetRuntimeContext.execute(
        enforcer, accessRecorder, principal, datasetInstanceId, getConstructorDefaultAnnotation(accessType), () ->
          LineageWriterDatasetFramework.super.getDataset(datasetInstanceId, arguments, classLoader,
                                                         classLoaderProvider, owners, accessType));
    } catch (IOException | DatasetManagementException | ServiceUnavailableException e) {
      throw e;
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to create dataset instance: " + datasetInstanceId, e);
    }
  }

  @Override
  public void writeLineage(DatasetId datasetInstanceId, AccessType accessType) {
    // No need to record lineage for system dataset. See getDataset call above.
    if (DatasetsUtil.isUserDataset(datasetInstanceId)) {
      super.writeLineage(datasetInstanceId, accessType);
      publishAudit(datasetInstanceId, accessType);
      doWriteLineage(datasetInstanceId, accessType);
    }
  }

  private void doWriteLineage(DatasetId datasetInstanceId, AccessType accessType) {
    ProgramContext programContext = this.programContext;
    if (programContext != null) {
      ProgramRunId programRunId = programContext.getProgramRunId();
      NamespacedEntityId componentId = programContext.getComponentId();
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

  private void publishAudit(DatasetId datasetInstanceId, AccessType accessType) {
    ProgramContext programContext = this.programContext;
    if (programContext != null) {
      ProgramRunId programRunId = programContext.getProgramRunId();
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

  /**
   * Returns the default authorization annotation for dataset constructor based on the access type.
   */
  @Nullable
  private Class<? extends Annotation> getConstructorDefaultAnnotation(AccessType accessType) {
    switch (accessType) {
      case READ:
        return ReadOnly.class;
      case WRITE:
        return WriteOnly.class;
      case READ_WRITE:
        return ReadWrite.class;
      case UNKNOWN:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported access type " + accessType);
    }
  }

  private final class BasicDatasetAccessRecorder implements DefaultDatasetRuntimeContext.DatasetAccessRecorder {

    private final AccessType requestedAccessType;
    private final DatasetId datasetInstanceId;

    @Nullable
    private final Iterable<? extends EntityId> owners;

    private BasicDatasetAccessRecorder(DatasetId datasetInstanceId, AccessType accessType,
                                       @Nullable Iterable<? extends EntityId> owners) {
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
        usageWriter.registerAll(owners, datasetInstanceId);
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
