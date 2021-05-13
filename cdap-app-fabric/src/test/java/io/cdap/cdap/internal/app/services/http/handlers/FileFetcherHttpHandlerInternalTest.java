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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;

/**
 * Test for {@link FileFetcherHttpHandlerInternal}.
 */
public class FileFetcherHttpHandlerInternalTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcherHttpHandlerInternalTest.class);
  private static NettyHttpService httpService;
  private static URL baseURL;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LocationFactory.class).to(LocalLocationFactory.class);
        }
      });

    FileFetcherHttpHandlerInternal handler = injector.getInstance(FileFetcherHttpHandlerInternal.class);

    httpService = NettyHttpService.builder("FileFetcherHttpHandlerInternalTest")
      .setHttpHandlers(handler)
      .build();

    httpService.start();

    InetSocketAddress addr = httpService.getBindAddress();
    baseURL = new URL(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
  }

  @AfterClass
  public static void stop() throws Exception {
    httpService.stop();
  }

  @Test
  public void testSuccess() throws Exception {
    // Create a file.
    File srcFile = TEMP_FOLDER.newFile("src_file");
    try (FileOutputStream out = new FileOutputStream(srcFile)) {
      byte[] bytes = new byte[256 * 1024 + 7];
      new Random().nextBytes(bytes);
      out.write(bytes);
    }

    // Download the file.
    File targetFile = new File(TEMP_FOLDER.newFolder(), "dst_file");
    Location dst = new LocalLocationFactory().create(targetFile.toURI());
    HttpResponse httpResponse = download(srcFile, dst);
    Assert.assertEquals(httpResponse.getResponseCode(), HttpResponseStatus.OK.code());

    // Verify the source and destination files are identical.
    byte[] orgFileContent = Files.readAllBytes(srcFile.toPath());
    byte[] downloadedFileContent = Files.readAllBytes(targetFile.toPath());
    Assert.assertArrayEquals(orgFileContent, downloadedFileContent);
  }

  @Test
  public void testFileNotFound() throws Exception {
    // A non-existent file
    File srcFile = new File(TEMP_FOLDER.getRoot(), "file_dont_exist");

    // Download the file.
    File targetFile = new File(TEMP_FOLDER.newFolder(), "dst_file");
    Location dst = new LocalLocationFactory().create(targetFile.toURI());

    HttpResponse httpResponse = download(srcFile, dst);
    Assert.assertEquals(httpResponse.getResponseCode(), HttpResponseStatus.NOT_FOUND.code());
  }

  private HttpResponse download(File src, Location dst) throws IOException {
    // Make a request to download the source file.
    URL url = new URL(String.format("%s/v3Internal/location/%s", baseURL, src.toURI().getPath()));
    OutputStream outputStream = dst.getOutputStream();
    HttpRequest request = HttpRequest.builder(
      HttpMethod.GET, url).withContentConsumer(
      new HttpContentConsumer() {
        @Override
        public boolean onReceived(ByteBuffer chunk) {
          try {
            byte[] bytes = new byte[chunk.remaining()];
            chunk.get(bytes, 0, bytes.length);
            outputStream.write(bytes);
            outputStream.flush();
          } catch (IOException e) {
            LOG.error("Failed to write to orgFile {}", dst.toURI());
            return false;
          }
          return true;
        }

        @Override
        public void onFinished() {
          try {
            outputStream.close();
          } catch (Exception e) {
            LOG.error("Failed to close to orgFile {}", dst.toURI());
          }
        }
      }).build();
    HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    httpResponse.consumeContent();
    return httpResponse;
  }

}
