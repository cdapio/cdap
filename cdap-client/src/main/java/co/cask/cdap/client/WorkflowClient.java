/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * Client to interact with workflows
 */
public class WorkflowClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .create();

  private final ClientConfig config;
  private final RESTClient restClient;

  @Inject
  public WorkflowClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  WorkflowClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Retrieve the entire {@link WorkflowToken} for the specified workflow run.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @return {@link WorkflowTokenDetail} for the specified workflow run
   */
  public WorkflowTokenDetail getWorkflowToken(Id.Run workflowRunId)
    throws UnauthorizedException, IOException, NotFoundException {
    return getWorkflowToken(workflowRunId, null, null);
  }

  /**
   * Retrieve all the keys in the {@link WorkflowToken} with the specified scope for the specified workflow run.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param scope the specified {@link WorkflowToken.Scope}
   * @return {@link WorkflowTokenDetail} containing all the keys in the specified {@link WorkflowToken.Scope}
   * for the specified workflow run
   */
  public WorkflowTokenDetail getWorkflowToken(Id.Run workflowRunId, WorkflowToken.Scope scope)
    throws UnauthorizedException, IOException, NotFoundException {
    return getWorkflowToken(workflowRunId, scope, null);
  }

  /**
   * Retrieve the specified key from the {@link WorkflowToken} for the specified workflow run.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param key the specified key
   * @return {@link WorkflowTokenDetail} containing all the values for the specified key
   */
  public WorkflowTokenDetail getWorkflowToken(Id.Run workflowRunId, String key)
    throws UnauthorizedException, IOException, NotFoundException {
    return getWorkflowToken(workflowRunId, null, key);
  }

  /**
   * Retrieve the {@link WorkflowToken} for the specified workflow run filtered by the specified
   * {@link WorkflowToken.Scope} and key.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param scope the specified {@link WorkflowToken.Scope}. If null, it returns keys for
   * {@link WorkflowToken.Scope#USER}
   * @param key the specified key. If null, it returns all keys in the specified {@link WorkflowToken.Scope}
   * @return {@link WorkflowTokenDetail} with the specified filters
   */
  public WorkflowTokenDetail getWorkflowToken(Id.Run workflowRunId, @Nullable WorkflowToken.Scope scope,
                                              @Nullable String key)
    throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/workflows/%s/runs/%s/token",
                                workflowRunId.getProgram().getApplicationId(), workflowRunId.getProgram().getId(),
                                workflowRunId.getId());
    URL url = config.resolveNamespacedURLV3(workflowRunId.getNamespace(), appendScopeAndKeyToUrl(path, scope, key));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      String msg = "Either the workflow or its run id";
      if (key != null) {
        msg = String.format("%s or the specified key at the specified scope", msg);
      }
      throw new NotFoundException(workflowRunId, msg);
    }
    return ObjectResponse.fromJsonBody(response, WorkflowTokenDetail.class, GSON).getResponseObject();
  }

  /**
   * Retrieve all the key-value pairs in a workflow run's {@link WorkflowToken} that were set by the specified node.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param nodeName the name of the node
   * @return {@link WorkflowTokenNodeDetail} containing all the key-value pairs set by the specified node in the
   * specified workflow run's {@link WorkflowToken}
   */
  public WorkflowTokenNodeDetail getWorkflowTokenAtNode(Id.Run workflowRunId, String nodeName)
    throws UnauthorizedException, IOException, NotFoundException {
    return getWorkflowTokenAtNode(workflowRunId, nodeName, null, null);
  }

  /**
   * Retrieve all the key-value pairs in a workflow run's {@link WorkflowToken} that were set by the specified node
   * in the specified {@link WorkflowToken.Scope}.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param nodeName the name of the node
   * @param scope the specified {@link WorkflowToken.Scope}
   * @return {@link WorkflowTokenNodeDetail} containing all the keys set by the specified node with the
   * specified {@link WorkflowToken.Scope}in the specified workflow run's {@link WorkflowToken}
   */
  public WorkflowTokenNodeDetail getWorkflowTokenAtNode(Id.Run workflowRunId, String nodeName,
                                                        WorkflowToken.Scope scope)
    throws UnauthorizedException, IOException, NotFoundException {
    return getWorkflowTokenAtNode(workflowRunId, nodeName, scope, null);
  }

  /**
   * Retrieve the specified key set by the specified node in a specified workflow run's {@link WorkflowToken}.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param nodeName the name of the node
   * @param key the specified key
   * @return {@link WorkflowTokenNodeDetail} containing the specified key set by the specified node with the
   * specified {@link WorkflowToken.Scope}in the specified workflow run's {@link WorkflowToken}
   */
  public WorkflowTokenNodeDetail getWorkflowTokenAtNode(Id.Run workflowRunId, String nodeName, String key)
    throws UnauthorizedException, IOException, NotFoundException {
    return getWorkflowTokenAtNode(workflowRunId, nodeName, null, key);
  }

  /**
   * Retrieve the keys set by the specified node in the {@link WorkflowToken} for the specified workflow run
   * filtered by the specified {@link WorkflowToken.Scope} and key.
   *
   * @param workflowRunId the {@link Id.Run} of the workflow
   * @param nodeName the name of the node
   * @param scope the specified {@link WorkflowToken.Scope}
   * @param key the specified key
   * @return {@link WorkflowTokenDetail} with the specified filters
   */
  public WorkflowTokenNodeDetail getWorkflowTokenAtNode(Id.Run workflowRunId, String nodeName,
                                                        @Nullable WorkflowToken.Scope scope, @Nullable String key)
    throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/workflows/%s/runs/%s/nodes/%s/token",
                                workflowRunId.getProgram().getApplicationId(), workflowRunId.getProgram().getId(),
                                workflowRunId.getId(), nodeName);
    URL url = config.resolveNamespacedURLV3(workflowRunId.getNamespace(), appendScopeAndKeyToUrl(path, scope, key));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      String msg = "Either the workflow or its run id";
      if (key != null) {
        msg = String.format("%s or the specified key at the specified scope", msg);
      }
      throw new NotFoundException(workflowRunId, msg);
    }
    return ObjectResponse.fromJsonBody(response, WorkflowTokenNodeDetail.class, GSON).getResponseObject();
  }

  private String appendScopeAndKeyToUrl(String workflowTokenUrl, @Nullable WorkflowToken.Scope scope, String key) {
    StringBuilder output = new StringBuilder(workflowTokenUrl);
    if (scope != null) {
      output.append(String.format("?scope=%s", scope.name()));
      if (key != null) {
        output.append(String.format("&key=%s", key));
      }
    } else if (key != null) {
      output.append(String.format("?key=%s", key));
    }
    return output.toString();
  }
}
