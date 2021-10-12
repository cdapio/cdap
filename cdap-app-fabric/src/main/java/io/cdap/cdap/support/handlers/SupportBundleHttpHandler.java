/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

package io.cdap.cdap.support.handlers;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Support Bundle HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractAppFabricHttpHandler {

  private final SupportBundleService supportBundleService;

  @Inject
  SupportBundleHttpHandler(SupportBundleService supportBundleService) {
    this.supportBundleService = supportBundleService;
  }

  /**
   * Generate the Support Bundle if valid application id, workflow id, and runid are provided.
   *
   * @param namespaceId  the namespace id
   * @param appId        the app id
   * @param workflowName the workflow name
   * @param runId        the runid of the workflow uuid of this support bundle
   * @param numOfRunLog  the num of run log for each pipeline run do they prefer
   */
  @POST
  @Path("/support/bundle")
  public void createSupportBundle(HttpRequest request,
                                  HttpResponder responder,
                                  @Nullable @QueryParam("namespace-id") String namespaceId,
                                  @Nullable @QueryParam("app-id") String appId,
                                  @Nullable @QueryParam("workflow-name") String workflowName,
                                  @Nullable @QueryParam("run-id") String runId,
                                  @Nullable @QueryParam("num-run-log") Integer numOfRunLog) {
    // Establishes the support bundle configuration
    SupportBundleConfiguration supportBundleConfiguration =
        new SupportBundleConfiguration(
            namespaceId, appId, runId, workflowName == null ? "DataPipelineWorkflow" : workflowName,
            numOfRunLog == null ? 1 : numOfRunLog);
    // Generates support bundle and returns with uuid
    String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
    responder.sendString(HttpResponseStatus.OK, uuid);
  }
}
