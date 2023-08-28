/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operations;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.api.service.operation.LongRunningOperation;
import io.cdap.cdap.api.service.operation.OperationError;
import io.cdap.cdap.api.service.operation.OperationMeta;
import io.cdap.cdap.api.service.operation.OperationRun;
import io.cdap.cdap.api.service.operation.OperationRunManager;
import io.cdap.cdap.api.service.operation.OperationStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class DefaultOperationRunManager<T> extends AbstractExecutionThreadService implements OperationRunManager<T> {
  private final String operationId;
  private final NamespaceId namespaceId;
  private final OperationRunsStore store;

  private static final Logger LOG = LoggerFactory.getLogger(DefaultOperationRunManager.class);


  public DefaultOperationRunManager(String operationId, NamespaceId namespaceId, OperationRunsStore store) {
    this.operationId = operationId;
    this.namespaceId = namespaceId;
    this.store = store;
  }

  @Override
  public void updateOperationStatus(OperationStatus status) {
    // retry couple of times and then we should just fail
    try {
      store.updateOperationStatus(namespaceId, operationId, status);
    } catch (Exception e){
      LOG.error("Failed to update the status");
    }
  }

  @Override
  public void updateOperationMeta(OperationMeta meta) {
    // retry couple of times and then we should just fail
    try {
      store.updateOperationMeta(namespaceId, operationId, meta);
    } catch (Exception e){
      LOG.error("Failed to update the status");
    }
  }

  @Override
  public void failOperation(List<OperationError> errors) {
    // retry couple of times and then we should just fail
    try {
      store.failOperation(namespaceId, operationId, errors);
    } catch (Exception e){
      LOG.error("Failed to update the status");
    }
  }

  @Override
  public OperationRun getOperation(String operationId) throws Exception {
      return store.getOperation(namespaceId, operationId);
  }

  @Override
  public void runOperation(LongRunningOperation<T> operation, T request) {
    updateOperationStatus(OperationStatus.RUNNING);
    try{
      List<OperationError> errors = operation.run(request, this::updateOperationMeta);
      if (!errors.isEmpty()){
        failOperation(errors);
      } else {
        updateOperationStatus(OperationStatus.SUCCEDED);
      }
    } catch (Exception e){
      String errMessage = String.format("Failed to run operation %s with request %s", operation.getType(), request);
      LOG.error(errMessage, e);
      List<OperationError> errs = Arrays.asList(new OperationError("", e.getMessage()));
      failOperation(errs);
    }
  }

  @Override
  protected void run() throws Exception {

  }
}
