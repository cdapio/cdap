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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.support.services.SupportBundleGenerator;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Threads;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Support Bundle HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractHttpHandler {

  private final CConfiguration cConf;
  private final SupportBundleGenerator bundleGenerator;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private ExecutorService executorService;

  @Inject
  SupportBundleHttpHandler(CConfiguration cConf,
                           SupportBundleGenerator bundleGenerator,
                           ContextAccessEnforcer contextAccessEnforcer) {
    this.cConf = cConf;
    this.bundleGenerator = bundleGenerator;
    this.contextAccessEnforcer = contextAccessEnforcer;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    executorService = Executors.newFixedThreadPool(cConf.getInt(Constants.SupportBundle.MAX_THREADS),
                                                   Threads.createDaemonThreadFactory("support-bundle-executor-%d"));
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    executorService.shutdownNow();
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
    // ensure the user is authorized to create supportBundle
    contextAccessEnforcer.enforceOnParent(EntityType.SUPPORT_BUNDLE, InstanceId.SELF, StandardPermission.CREATE);
    // Establishes the support bundle configuration
    SupportBundleConfiguration bundleConfig =
      new SupportBundleConfiguration(namespace, application, run, ProgramType.valueOfCategoryName(programType),
                                     programName, Optional.ofNullable(maxRunsPerProgram).orElse(1));
    // Generates support bundle and returns with uuid
    String uuid = bundleGenerator.generate(bundleConfig, executorService);
    responder.sendString(HttpResponseStatus.OK, uuid);
  }
}
