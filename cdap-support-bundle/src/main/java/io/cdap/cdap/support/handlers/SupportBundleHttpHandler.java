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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.SupportBundleEntityId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.support.lib.SupportBundleOperationStatus;
import io.cdap.cdap.support.lib.SupportBundleRequestFileList;
import io.cdap.cdap.support.services.SupportBundleGenerator;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.twill.common.Threads;

/**
 * Support Bundle HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final String APPLICATION_ZIP = "application/zip";
  private static final String CONTENT_DISPOSITION_VALUE = "attachment; filename=\"bundle-%d.zip\"";

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
    executorService = Executors.newFixedThreadPool(
        cConf.getInt(Constants.SupportBundle.MAX_THREADS),
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
  @Path("/support/bundles")
  public void createSupportBundle(HttpRequest request, HttpResponder responder,
      @Nullable @QueryParam("namespace") String namespace,
      @Nullable @QueryParam("application") String application,
      @Nullable @QueryParam("programType") @DefaultValue("workflows") String programType,
      @Nullable @QueryParam("programId") @DefaultValue("DataPipelineWorkflow")
          String programName, @Nullable @QueryParam("run") String run,
      @Nullable @QueryParam("maxRunsPerProgram") @DefaultValue("1")
          Integer maxRunsPerProgram) throws Exception {
    // ensure the user is authorized to create supportBundle
    contextAccessEnforcer.enforceOnParent(EntityType.SUPPORT_BUNDLE, InstanceId.SELF,
        StandardPermission.CREATE);
    // Establishes the support bundle configuration
    SupportBundleConfiguration bundleConfig =
        new SupportBundleConfiguration(namespace, application, run,
            ProgramType.valueOfCategoryName(programType),
            programName, Optional.ofNullable(maxRunsPerProgram).orElse(1));
    // Generates support bundle and returns with uuid
    String prevInProgressUUID = bundleGenerator.getInProgressBundle();
    if (prevInProgressUUID == null) {
      // Generates support bundle and returns with uuid
      String uuid = bundleGenerator.generate(bundleConfig, executorService);
      responder.sendString(HttpResponseStatus.CREATED, uuid);
    } else {
      responder.sendString(HttpResponseStatus.OK,
          String.format("The prev bundle id: %s is still running.", prevInProgressUUID));
    }
  }

  /**
   * Get the list of bundle status from status.json.
   *
   * @throws NotFoundException is thrown if status.json is not found
   */
  @GET
  @Path("/support/bundles")
  public void listSupportBundles(HttpRequest request, HttpResponder responder) {
    // Ensure the user is authorized to list support bundle statuses
    contextAccessEnforcer.enforceOnParent(EntityType.SUPPORT_BUNDLE, InstanceId.SELF,
        StandardPermission.LIST);

    List<SupportBundleOperationStatus> bundleOperationStatusList = bundleGenerator.getAllBundleStatus();
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(bundleOperationStatusList));
  }

  /**
   * Get the specific bundle status from status.json by using uuid.
   *
   * @param uuid the bundle id which is also the uuid
   * @throws NotFoundException is thrown if status.json is not found
   */
  @GET
  @Path("/support/bundles/{uuid}/status")
  public void getSupportBundleStatus(HttpRequest request, HttpResponder responder,
      @PathParam("uuid") String uuid)
      throws IOException, NotFoundException {
    SupportBundleEntityId bundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(bundleEntityId, StandardPermission.GET);
    SupportBundleOperationStatus bundleOperationStatus = bundleGenerator.getBundle(uuid);
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(bundleOperationStatus));
  }

  /**
   * Delete the specific bundle by using the uuid
   *
   * @param uuid the bundle id which is also the uuid
   * @throws NotFoundException is thrown if status.json is not found
   */
  @DELETE
  @Path("/support/bundles/{uuid}")
  public void deleteSupportBundle(HttpRequest request, HttpResponder responder,
      @PathParam("uuid") String uuid)
      throws IOException, NotFoundException {
    SupportBundleEntityId bundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(bundleEntityId, StandardPermission.DELETE);
    bundleGenerator.deleteBundle(uuid);
    responder.sendString(HttpResponseStatus.OK,
        String.format("Successfully deleted bundle %s", uuid));
  }

  /**
   * Exports all selected files as a ZIP archive file.
   *
   * @param uuid the bundle id which is also the uuid fileJson body
   */
  @POST
  @Path("/support/bundles/{uuid}")
  public void downloadSupportBundle(FullHttpRequest request, HttpResponder responder,
      @PathParam("uuid") String uuid)
      throws BadRequestException, IOException, NotFoundException, NoSuchAlgorithmException {
    SupportBundleEntityId bundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(bundleEntityId, StandardPermission.GET);
    String requestContent = request.content().toString(StandardCharsets.UTF_8);
    File tempDir = new File(
        cConf.get(Constants.SupportBundle.SUPPORT_BUNDLE_TEMP_DIR)).getAbsoluteFile();
    DirUtils.mkdirs(tempDir);
    java.nio.file.Path tmpPath = Files.createTempFile(tempDir.toPath(), "support-bundle", ".zip");
    String digestHeader = "";
    SupportBundleRequestFileList bundleRequestFileList;
    try {
      if (requestContent == null || requestContent.length() == 0) {
        bundleRequestFileList = new SupportBundleRequestFileList(new ArrayList<>());
        digestHeader = bundleGenerator.createBundleZipByRequest(uuid, tmpPath,
            bundleRequestFileList);
      } else {
        try {
          bundleRequestFileList = GSON.fromJson(requestContent, SupportBundleRequestFileList.class);
        } catch (Exception e) {
          throw new BadRequestException(
              String.format("Failed to parse body on %s", requestContent));
        }
        digestHeader = bundleGenerator.createBundleZipByRequest(uuid, tmpPath,
            bundleRequestFileList);
      }
      responder.sendFile(tmpPath.toFile(), new DefaultHttpHeaders().add("digest", digestHeader)
          .add(HttpHeaderNames.CONTENT_TYPE, APPLICATION_ZIP)
          .add(HttpHeaderNames.CONTENT_DISPOSITION,
              String.format(CONTENT_DISPOSITION_VALUE, System.currentTimeMillis())));
    } finally {
      Files.deleteIfExists(tmpPath);
    }
  }
}
