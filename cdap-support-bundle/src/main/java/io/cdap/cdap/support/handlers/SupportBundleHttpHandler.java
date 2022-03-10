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
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.SupportBundleEntityId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.support.lib.SupportBundleExportRequest;
import io.cdap.cdap.support.lib.SupportBundleFiles;
import io.cdap.cdap.support.lib.SupportBundleOperationStatus;
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
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Support Bundle HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SupportBundleHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleHttpHandler.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(org.apache.twill.internal.Arguments.class, new org.apache.twill.internal.json.ArgumentsCodec())
    .create();
  private static final String applicationZip = "application/zip";
  private static final String contentDescriptionValue = "attachment; filename=\"collect_support_bundle.zip\"";

  private final CConfiguration cConf;
  private final SupportBundleGenerator bundleGenerator;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private ExecutorService executorService;

  @Inject
  SupportBundleHttpHandler(SupportBundleGenerator bundleGenerator, ContextAccessEnforcer contextAccessEnforcer,
                           CConfiguration cConf) {
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

  /**
   * Get the list of bundle status from status.json.
   *
   * @throws IOException is thrown if status.json is not found
   */
  @GET
  @Path("/support/bundle")
  public void getSupportBundle(HttpRequest request, HttpResponder responder) throws Exception {
    /** ensure the user has authentication to create supportBundle */
    contextAccessEnforcer.enforceOnParent(EntityType.SUPPORT_BUNDLE, InstanceId.SELF, StandardPermission.LIST);
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    if (!baseDirectory.exists()) {
      responder.sendString(HttpResponseStatus.OK, "No content in Support Bundle.");
      return;
    }
    List<SupportBundleOperationStatus> supportBundleOperationStatusList = new ArrayList<>();
    File[] supportFiles =
      baseDirectory.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
    if (supportFiles != null && supportFiles.length > 0) {
      for (File uuidFile : supportFiles) {
        SupportBundleOperationStatus supportBundleOperationStatus =
          bundleGenerator.getSingleBundleJson(uuidFile.getName());
        supportBundleOperationStatusList.add(supportBundleOperationStatus);
      }
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(supportBundleOperationStatusList));
  }

  /**
   * Get the specific bundle status from status.json by using uuid.
   *
   * @param uuid the bundle id which is also the uuid
   * @throws IOException is thrown if status.json is not found
   */
  @GET
  @Path("/support/bundle/{uuid}")
  public void getSupportBundleByUUID(HttpRequest request, HttpResponder responder, @PathParam("uuid") String uuid)
    throws Exception {
    SupportBundleEntityId supportBundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(supportBundleEntityId, StandardPermission.GET);
    List<SupportBundleOperationStatus> supportBundleOperationStatusList = new ArrayList<>();
    SupportBundleOperationStatus supportBundleOperationStatus = bundleGenerator.getSingleBundleJson(uuid);
    supportBundleOperationStatusList.add(supportBundleOperationStatus);
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(supportBundleOperationStatusList));
  }

  /**
   * Delete the specific bundle by using the uuid
   *
   * @param uuid the bundle id which is also the uuid
   * @throws IOException is thrown if status.json is not found
   */
  @DELETE
  @Path("/support/bundle/{uuid}")
  public void deleteSupportBundle(HttpRequest request, HttpResponder responder, @PathParam("uuid") String uuid)
    throws Exception {
    SupportBundleEntityId supportBundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(supportBundleEntityId, StandardPermission.DELETE);
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(baseDirectory, uuid);
    if (!baseDirectory.exists()) {
      responder.sendString(HttpResponseStatus.OK, "No content in Support Bundle.");
      return;
    }
    if (!uuidFile.exists()) {
      responder.sendString(HttpResponseStatus.OK, String.format("No such uuid '%s' in Support Bundle.", uuid));
      return;
    }
    bundleGenerator.deleteOldFolders(uuidFile);
    responder.sendString(HttpResponseStatus.OK, String.format("Successfully deleted bundle %s", uuid));
  }

  /**
   * Get the specific bundle file list or certain file content
   *
   * @param uuid the bundle id which is also the uuid
   * @throws IOException is thrown if status.json is not found
   */
  @GET
  @Path("/support/bundle/{uuid}/files")
  public void getSupportBundleFile(HttpRequest request, HttpResponder responder, @PathParam("uuid") String uuid,
                                   @DefaultValue("") @QueryParam("folder-name") String folderName,
                                   @DefaultValue("") @QueryParam("data-file-name") String dataFileName)
    throws IOException {
    SupportBundleEntityId supportBundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(supportBundleEntityId, StandardPermission.GET);
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    File uuidFile = new File(baseDirectory, uuid);
    if (!baseDirectory.exists()) {
      responder.sendString(HttpResponseStatus.OK, "No content in Support Bundle.");
      return;
    }
    if (!uuidFile.exists()) {
      responder.sendString(HttpResponseStatus.OK, String.format("No such uuid '%s' in Support Bundle.", uuid));
      return;
    }
    if (folderName == null || folderName.length() == 0 || dataFileName == null || dataFileName.length() == 0) {
      File[] pipelineFiles =
        uuidFile.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
      List<SupportBundleFiles> supportBundleFilesList = new ArrayList<>();
      if (pipelineFiles != null && pipelineFiles.length > 0) {
        for (File pipelineFile : pipelineFiles) {
          if (folderName != null && folderName.length() > 0) {
            if (pipelineFile.getName().equals(folderName)) {
              SupportBundleFiles supportBundleFiles = bundleGenerator.getFilesName(pipelineFile);
              responder.sendString(HttpResponseStatus.OK, GSON.toJson(supportBundleFiles));
              return;
            }
          } else {
            SupportBundleFiles supportBundleFiles = bundleGenerator.getFilesName(pipelineFile);
            if (supportBundleFiles != null) {
              supportBundleFilesList.add(supportBundleFiles);
            }
          }
        }
      } responder.sendString(HttpResponseStatus.OK, GSON.toJson(supportBundleFilesList));
      return;
    } File folderDirectory = new File(uuidFile, folderName);
    File[] dataFiles =
      folderDirectory.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
    if (dataFiles != null && dataFiles.length > 0) {
      for (File dataFile : dataFiles) {
        if (dataFile.getName().startsWith(dataFileName)) {
          BufferedReader br = new BufferedReader(new FileReader(dataFile));
          StringBuilder sb = new StringBuilder();
          String line = br.readLine();
          while (line != null) {
            sb.append(line).append("\n");
            line = br.readLine();
          }
          responder.sendString(HttpResponseStatus.OK, sb.toString());
          return;
        }
      }
    }
    responder.sendString(HttpResponseStatus.NOT_FOUND,
                         String.format("uuid: %s with pipeline file name: %s and data file name: %s not found", uuid,
                                       folderName, dataFileName));
  }

  /**
   * Exports all selected files as a ZIP archive file.
   *
   * @param uuid the bundle id which is also the uuid fileJson body
   */
  @POST
  @Path("/support/bundle/{uuid}/download")
  public void appsExport(FullHttpRequest request, HttpResponder responder, @PathParam("uuid") String uuid)
    throws Exception {
    SupportBundleEntityId supportBundleEntityId = new SupportBundleEntityId(uuid);
    contextAccessEnforcer.enforce(supportBundleEntityId, StandardPermission.GET);
    String requestContent = request.content().toString(StandardCharsets.UTF_8);
    if (requestContent == null || requestContent.length() == 0) {
      throw new BadRequestException("Request body is empty.");
    }
    SupportBundleExportRequest supportBundleExportRequest =
      GSON.fromJson(requestContent, SupportBundleExportRequest.class);
    File tempDir = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR),
                            cConf.get(Constants.SupportBundle.SUPPORT_BUNDLE_TEMP_DIR)).getAbsoluteFile();
    DirUtils.mkdirs(tempDir);
    java.nio.file.Path tmpPath = Files.createTempFile(tempDir.toPath(), "collect_support_bundle", ".zip");
    List<String> listOfRequestFilePath = new ArrayList<>();
    if (supportBundleExportRequest.getSupportBundleRequestFileList().getFiles().size() > 0) {
      List<String> fileList = supportBundleExportRequest.getSupportBundleRequestFileList().getFiles();
      fileList.iterator().forEachRemaining(files -> listOfRequestFilePath.add(files));
    }
    File uuidFile = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR), uuid);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (ZipOutputStream zipOut = new ZipOutputStream(
        new DigestOutputStream(Files.newOutputStream(tmpPath, StandardOpenOption.TRUNCATE_EXISTING), digest))) {
        if (uuidFile.exists()) {
          for (String filePath : listOfRequestFilePath) {
            String uuidFilePath = uuid + "/" + filePath;
            File file = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR), uuidFilePath);
            if (file.exists()) {
              ZipEntry entry = new ZipEntry(uuidFile.getName() + "/" + filePath);
              zipOut.putNextEntry(entry);
              Files.copy(file.toPath(), zipOut);
              zipOut.closeEntry();
            }
          }
        }
      }

      responder.sendFile(tmpPath.toFile(), new DefaultHttpHeaders().add("digest", String.format("%s=%s",
                                                                                                digest.getAlgorithm()
                                                                                                  .toLowerCase(),
                                                                                                Base64.getEncoder()
                                                                                                  .encodeToString(
                                                                                                    digest.digest())))
        .add(HttpHeaderNames.CONTENT_TYPE, applicationZip)
        .add(HttpHeaderNames.CONTENT_DISPOSITION, contentDescriptionValue));
    } finally {
      Files.deleteIfExists(tmpPath);
    }
  }
}
