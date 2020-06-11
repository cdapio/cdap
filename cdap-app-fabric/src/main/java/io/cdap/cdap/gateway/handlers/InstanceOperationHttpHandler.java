/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.zip.ZipOutputStream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * HTTP handler for instance level operations.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class InstanceOperationHttpHandler extends AbstractHttpHandler {

  private final CConfiguration cConf;
  private final ApplicationLifecycleService lifecycleService;

  @Inject
  InstanceOperationHttpHandler(CConfiguration cConf, ApplicationLifecycleService lifecycleService) {
    this.cConf = cConf;
    this.lifecycleService = lifecycleService;
  }

  /**
   * Exports all application details as a ZIP archive file.
   */
  @GET
  @Path("/export/apps")
  public void appsExport(HttpRequest request, HttpResponder responder) throws Exception {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    DirUtils.mkdirs(tempDir);
    java.nio.file.Path tmpPath = Files.createTempFile(tempDir.toPath(), "export", ".zip");
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (ZipOutputStream zipOut = new ZipOutputStream(new DigestOutputStream(
        Files.newOutputStream(tmpPath, StandardOpenOption.TRUNCATE_EXISTING), digest))) {

        lifecycleService.createAppDetailsArchive(zipOut);
      }

      responder.sendFile(
        tmpPath.toFile(),
        new DefaultHttpHeaders()
          .add("digest", String.format("%s=%s", digest.getAlgorithm().toLowerCase(),
                                       Base64.getEncoder().encodeToString(digest.digest())))
          .add(HttpHeaderNames.CONTENT_TYPE, "application/zip")
          .add(HttpHeaderNames.CONTENT_DISPOSITION,
               String.format("attachment; filename=\"export-%d.zip\"", System.currentTimeMillis()))
      );
    } finally {
      Files.deleteIfExists(tmpPath);
    }
  }
}
