/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.SupportBundlePermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Support Bundle HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractAppFabricHttpHandler {

  private final SupportBundleService bundleService;
  private final ContextAccessEnforcer contextAccessEnforcer;

  @Inject
  SupportBundleHttpHandler(SupportBundleService supportBundleService, ContextAccessEnforcer contextAccessEnforcer) {
    this.bundleService = supportBundleService;
    this.contextAccessEnforcer = contextAccessEnforcer;
  }

  /**
   * Trigger the Support Bundle Generation.
   *
   * @param namespace the namespace id
   * @param application the app id
   * @param programType the program type
   * @param programName the program name
   * @param run the runid of the workflow uuid of this support bundle
   * @param maxRunsPerProgram the max num of run log for each pipeline do they prefer
   */
  @POST
  @Path("/support/bundle")
  public void createSupportBundle(HttpRequest request, HttpResponder responder,
                                  @Nullable @QueryParam("namespace") String namespace,
                                  @Nullable @QueryParam("application") String application,
                                  @Nullable @QueryParam("programType") @DefaultValue("workflows") String programType,
                                  @Nullable @QueryParam("programId") @DefaultValue("DataPipelineWorkflow")
                                    String programName, @Nullable @QueryParam("run") String run,
                                  @Nullable @QueryParam("maxRunsPerProgram") @DefaultValue("1")
                                    Integer maxRunsPerProgram) throws Exception {
    /** ensure the user has authentication to create supportBundle */
    contextAccessEnforcer.enforceOnParent(EntityType.SUPPORT_BUNDLE, InstanceId.SELF,
                                          SupportBundlePermission.GENERATE_SUPPORT_BUNDLE);
    // Establishes the support bundle configuration
    SupportBundleConfiguration bundleConfig =
      new SupportBundleConfiguration(namespace, application, run, ProgramType.valueOfCategoryName(programType),
                                     programName, maxRunsPerProgram);
    // Generates support bundle and returns with uuid
    String uuid = bundleService.generateSupportBundle(bundleConfig);
    responder.sendString(HttpResponseStatus.OK, uuid);
  }
}
