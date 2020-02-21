/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.cdap.etl.batch.customaction;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.customaction.CustomActionContext;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.AbstractStageContext;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation for the {@link ActionContext}.
 */
public class BasicActionContext extends AbstractStageContext implements ActionContext  {
  private static final Logger LOG = LoggerFactory.getLogger(BasicActionContext.class);
  private static final String EXTERNAL_DATASET_TYPE = "externalDataset";

  private final CustomActionContext context;
  private final Admin admin;

  public BasicActionContext(CustomActionContext context, PipelineRuntime pipelineRuntime, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec);
    this.context = context;
    this.admin = context.getAdmin();
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    context.execute(runnable);
  }

  @Override
  public void execute(int timeout, TxRunnable runnable) throws TransactionFailureException {
    context.execute(timeout, runnable);
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    return context.list(namespace);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    return context.get(namespace, name);
  }

  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
                  Map<String, String> properties) throws Exception {
    context.getAdmin().put(namespace, name, data, description, properties);
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    context.getAdmin().delete(namespace, name);
  }

  @Override
  public void record(List<FieldOperation> fieldOperations) {
    throw new UnsupportedOperationException("Lineage recording is not supported.");
  }

  @Override
  public void registerLineage(String referenceName, AccessType accessType) throws DatasetManagementException {
    try {
      if (!admin.datasetExists(referenceName)) {
        admin.createDataset(referenceName, EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
      }
    } catch (InstanceConflictException ex) {
      // Might happen if there is executed in multiple drivers in parallel. A race condition exists between check
      // for dataset existence and creation.
      LOG.debug("Dataset with name {} already created. Hence not creating the external dataset.", referenceName);
    }

    Transactionals.execute(context, context -> {
      // we cannot instantiate ExternalDataset here - it is in CDAP data-fabric,
      // and this code (the pipeline app) cannot depend on that. Thus, use reflection
      // to invoke a method on the dataset.
      Dataset ds = context.getDataset(referenceName);
      Class<? extends Dataset> dsClass = ds.getClass();

      switch (accessType) {
        case READ:
          invokeMethod(referenceName, ds, dsClass, "recordRead", accessType);
          break;
        case WRITE:
          invokeMethod(referenceName, ds, dsClass, "recordWrite", accessType);
          break;
        default:
          LOG.warn("Failed to register lineage because of unknown access type {}", accessType);
      }
    }, DatasetManagementException.class);
  }

  private void invokeMethod(String referenceName, Dataset ds, Class<? extends Dataset> dsClass, String methodName,
                            AccessType accessType) throws IllegalAccessException, InvocationTargetException {
    try {
      Method method = dsClass.getMethod(methodName);
      method.invoke(ds);
    } catch (NoSuchMethodException e) {
      LOG.warn("ExternalDataset '{}' does not have method '{}'. " +
                 "Can't register {} lineage for this dataset", referenceName, methodName, accessType);
    }
  }

  @Override
  public void record(Collection<? extends Operation> operations) {
    context.record(operations);
  }

  @Override
  public void flushLineage() {
    context.flushLineage();
  }
}
