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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Support Bundle HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleHttpHandler.class);
  private final CConfiguration cConf;
  private final SupportBundleService supportBundleService;

  @Inject
  SupportBundleHttpHandler(CConfiguration cConf,
                           SupportBundleService supportBundleService) {
    this.cConf = cConf;
    this.supportBundleService = supportBundleService;
  }

  /**
   * Trigger the Support Bundle Generation.
   *
   * @param namespaceId        the namespace id
   * @param appId              the app id
   * @param workflowName       the workflow name
   * @param runId              the runid of the workflow uuid of this support bundle
   * @param maxRunsPerPipeline the max num of run log for each pipeline do they prefer
   */
  @POST
  @Path("/support/bundle")
  public void createSupportBundle(HttpRequest request,
                                  HttpResponder responder,
                                  @Nullable @QueryParam("namespace-id") String namespaceId,
                                  @Nullable @QueryParam("app-id") String appId,
                                  @Nullable @QueryParam("workflow-name") String workflowName,
                                  @Nullable @QueryParam("run-id") String runId,
                                  @Nullable @QueryParam("max-runs-per-pipeline")
                                      Integer maxRunsPerPipeline) {
    // Establishes the support bundle configuration
    try {
      SupportBundleConfiguration supportBundleConfiguration =
          new SupportBundleConfiguration(
              namespaceId, appId, runId,
              workflowName == null ? cConf.get(SupportBundle.DEFAULT_WORKFLOW) : workflowName,
              maxRunsPerPipeline == null ? 1 : maxRunsPerPipeline);
      // Generates support bundle and returns with uuid
      String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
      responder.sendString(HttpResponseStatus.OK, uuid);
    } catch (Exception e) {
      LOG.error("Can not trigger support bundle generation ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }
}
