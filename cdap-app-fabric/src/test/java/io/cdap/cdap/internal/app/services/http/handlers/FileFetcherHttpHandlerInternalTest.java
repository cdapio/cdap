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

package io.cdap.cdap.internal.app.services.http.handlers;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.bouncycastle.util.Arrays;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Base64;

/**
 * Test for {@link FileFetcherHttpHandlerInternal}.
 */
public class FileFetcherHttpHandlerInternalTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcherHttpHandlerInternalTest.class);

  private static DiscoveryServiceClient discoveryServiceClient;

  @BeforeClass
  public static void setup() {
    discoveryServiceClient = getInjector().getInstance(DiscoveryServiceClient.class);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testDownloadFile() throws IOException {
    // Create a jar file
    LocalLocationFactory locationFactory = new LocalLocationFactory(tmpFolder.newFolder());
    Location location = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);
    File orgFile = new File(location.toURI().getPath());

    // Set the target file path to store downloaded file content.
    File targetFile = new File(tmpFolder.newFolder(), "target.jar");
    Location targetLocation = new LocalLocationFactory().create(targetFile.toURI());

    // Download file from AppFabric
    RemoteClient remoteClient = new RemoteClient(
      discoveryServiceClient,
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.INTERNAL_API_VERSION_3);
    OutputStream outputStream = targetLocation.getOutputStream();

    HttpRequest request = remoteClient.requestBuilder(
      HttpMethod.GET,
      String.format("location/%s", Base64.getEncoder().encodeToString(location.toURI().toString().getBytes())))
      .withContentConsumer(
      new HttpContentConsumer() {
      @Override
      public boolean onReceived(ByteBuffer chunk) {
        try {
          byte[] bytes = new byte[chunk.remaining()];
          chunk.get(bytes, 0, bytes.length);
          outputStream.write(bytes);
          outputStream.flush();
        } catch (IOException e) {
          LOG.error("Failed to write to orgFile {}", targetLocation.toURI());
          return false;
        }
        return true;
      }

      @Override
      public void onFinished() {
        try {
        outputStream.close();
        } catch (Exception e) {
          LOG.error("Failed to close to orgFile {}", targetLocation.toURI());
        }
      }
    }).build();
    HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    httpResponse.consumeContent();
    Assert.assertEquals(httpResponse.getResponseCode(), HttpResponseStatus.OK.code());

    // Verify the source and destination files are identical.
    byte[] orgFileContent = Files.readAllBytes(orgFile.toPath());
    byte[] downloadedFileContent = Files.readAllBytes(targetFile.toPath());
    Assert.assertEquals(orgFileContent.length, downloadedFileContent.length);
    Assert.assertTrue(Arrays.areEqual(orgFileContent, downloadedFileContent));
  }
}
