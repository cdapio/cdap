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
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.support.conf.SupportBundleConfiguration;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/** Support Bundle HTTP Handler. */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleHttpHandler.class);
  private final SupportBundleService supportBundleService;
  private final SupportBundleConfiguration supportBundleConfiguration;

  @Inject
  SupportBundleHttpHandler(
      SupportBundleService supportBundleService,
      SupportBundleConfiguration supportBundleConfiguration) {
    this.supportBundleService = supportBundleService;
    this.supportBundleConfiguration = supportBundleConfiguration;
  }

  /**
   * Generate the Support Bundle if valid application id, workflow id, and runid are provided.
   *
   * @param namespaceId the namespace id
   * @param appId the app id
   * @param workflowName the workflow name
   * @param runId the runid of the workflow uuid of this support bundle
   * @param numOfRunLog the num of run log for each pipeline run do they prefer
   */
  @POST
  @Path("/support/bundle")
  public void createSupportBundle(
      HttpResponder responder,
      @Nullable @QueryParam("namespace-id") String namespaceId,
      @Nullable @QueryParam("app-id") String appId,
      @Nullable @QueryParam("workflow-name") String workflowName,
      @Nullable @QueryParam("run-id") String runId,
      @Nullable @QueryParam("num-run-log") Integer numOfRunLog) {
    // Generate a universal unique id for each bundle and return to the front end right away
    supportBundleConfiguration.setNamespaceId(namespaceId);
    supportBundleConfiguration.setWorkflowName(workflowName);
    supportBundleConfiguration.setNumOfRunLogNeeded(numOfRunLog);
    supportBundleConfiguration.setNamespaceId(namespaceId);
    String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
    responder.sendString(
        HttpResponseStatus.OK, String.format("Support Bundle %s generated.", uuid));
  }
}
