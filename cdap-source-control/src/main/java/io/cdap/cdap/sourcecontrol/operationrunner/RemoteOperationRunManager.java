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

package io.cdap.cdap.sourcecontrol.operationrunner;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.gson.Gson;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.api.service.operation.LongRunningOperation;
import io.cdap.cdap.api.service.operation.OperationError;
import io.cdap.cdap.api.service.operation.OperationMeta;
import io.cdap.cdap.api.service.operation.OperationRun;
import io.cdap.cdap.api.service.operation.OperationRunManager;
import io.cdap.cdap.api.service.operation.OperationStatus;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RemoteOperationRunManager<T> extends AbstractExecutionThreadService implements OperationRunManager<T>  {
  private final String operationId;
  private final NamespaceId namespaceId;
  private final RemoteClient remoteClient;
  private final LongRunningOperation<T> operation;
  private final T request;

  private static final Logger LOG = LoggerFactory.getLogger(RemoteOperationRunManager.class);
  private static final Gson GSON = new Gson();

  public RemoteOperationRunManager(String operationId, NamespaceId namespaceId, RemoteClient remoteClient, LongRunningOperation<T> operation, T request) {
    this.operationId = operationId;
    this.namespaceId = namespaceId;
    this.remoteClient = remoteClient;
    this.operation = operation;
    this.request = request;
  }

  @Override
  public void updateOperationStatus(OperationStatus status) {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.POST,
        String.format("namespaces/%s/operations/%s/updateStatus", namespaceId.getNamespace(), operationId)
      ).withBody(GSON.toJson(status));

    try {
      HttpResponse response = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
      if(response.getResponseCode() != 200) {
        throw new RuntimeException("failed update status");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateOperationMeta(OperationMeta meta) {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.POST,
        String.format("namespaces/%s/operations/%s/updateMeta", namespaceId.getNamespace(), operationId)
      ).withBody(GSON.toJson(meta));

    try {
      HttpResponse response = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
      if(response.getResponseCode() != 200) {
        throw new RuntimeException("failed update status");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void failOperation(List<OperationError> errors) {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.POST,
        String.format("namespaces/%s/operations/%s/stop", namespaceId.getNamespace(), operationId)
      ).withBody(GSON.toJson(errors));

    try {
      HttpResponse response = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
      if(response.getResponseCode() != 200) {
        throw new RuntimeException("failed update status");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OperationRun getOperation(String operationId) throws Exception {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.GET,
        String.format("namespaces/%s/operations/%s", namespaceId.getNamespace(), operationId)
      );

      HttpResponse response = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
      if(response.getResponseCode() != 200) {
        throw new RuntimeException("failed update status");
      }
      return GSON.fromJson(response.getResponseBodyAsString(), OperationRun.class);
  }

  @Override
  public void runOperation(LongRunningOperation<T> operation, T request) {
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
  public void run() {
    runOperation(operation, request);
  }
}
