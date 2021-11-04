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
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
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
  private final SupportBundleService bundleService;

  @Inject
  SupportBundleHttpHandler(SupportBundleService supportBundleService) {
    this.bundleService = supportBundleService;
  }

  /**
   * Trigger the Support Bundle Generation.
   *
   * @param namespaceId the namespace id
   * @param appId the app id
   * @param programType the program type
   * @param programName the program name
   * @param runId the runid of the workflow uuid of this support bundle
   * @param maxRunsPerProgram the max num of run log for each pipeline do they prefer
   */
  @POST
  @Path("/support/bundle")
  public void createSupportBundle(HttpRequest request, HttpResponder responder,
                                  @Nullable @QueryParam("namespace") String namespaceId,
                                  @Nullable @QueryParam("app") String appId,
                                  @Nonnull @QueryParam("programType") String programType,
                                  @Nonnull @QueryParam("programId") String programName,
                                  @Nullable @QueryParam("run") String runId,
                                  @Nullable @QueryParam("maxRunsPerProgram") Integer maxRunsPerProgram) {
    // Establishes the support bundle configuration
    try {
      SupportBundleConfiguration bundleConfig =
        new SupportBundleConfiguration(namespaceId, appId, runId, programType, programName,
                                       maxRunsPerProgram == null ? 1 : maxRunsPerProgram);
      // Generates support bundle and returns with uuid
      String uuid = bundleService.generateSupportBundle(bundleConfig);
      responder.sendString(HttpResponseStatus.OK, uuid);
    } catch (Exception e) {
      LOG.error("Failed to trigger support bundle generation ", e);
      if (e instanceof HttpErrorStatusProvider) {
        responder.sendString(HttpResponseStatus.valueOf(((HttpErrorStatusProvider) e).getStatusCode()), e.getMessage());
      }
    }
  }
}
