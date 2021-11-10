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
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
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
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  SupportBundleHttpHandler(AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext,
                           SupportBundleService supportBundleService) {
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.bundleService = supportBundleService;
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
    ensureVisibilityOnProgram(namespace, application, programType, programName, run);
    // Establishes the support bundle configuration
    SupportBundleConfiguration bundleConfig =
      new SupportBundleConfiguration(namespace, application, run, ProgramType.valueOfCategoryName(programType),
                                     programName, maxRunsPerProgram);
    // Generates support bundle and returns with uuid
    String uuid = bundleService.generateSupportBundle(bundleConfig);
    responder.sendString(HttpResponseStatus.OK, uuid);
  }

  /** ensure the user has visibility for the query request they made */
  private void ensureVisibilityOnProgram(String namespace, String application, String programType, String programName,
                                         String run) throws Exception {
    if (namespace != null) {
      if (application != null) {
        if (run == null) {
          ProgramId programId =
            new ProgramId(namespace, application, ProgramType.valueOfCategoryName(programType), programName);
          accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
        } else {
          ProgramRunId programRunId =
            new ProgramRunId(namespace, application, ProgramType.valueOfCategoryName(programType), programName, run);
          accessEnforcer.enforce(programRunId, authenticationContext.getPrincipal(), StandardPermission.GET);
        }
      } else {
        NamespaceId namespaceId =
          new NamespaceId(namespace);
        accessEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), StandardPermission.GET);
      }
    }
  }
}
