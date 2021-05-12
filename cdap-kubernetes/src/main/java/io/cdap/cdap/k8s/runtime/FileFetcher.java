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

package io.cdap.cdap.k8s.runtime;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Download file from AppFabric via internal REST API calls.
 *
 * The target file could be either residing in AppFabric's local file system or a distributed file system
 * that AppFabric is configured to access.
 */
public class FileFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcher.class);
  private RemoteClient remoteClient = null;

  public FileFetcher(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient,
                                         Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Download a file from AppFabric and store it in the target file.
   * @param sourceURI uri to identity the file to download. This URI should exist in AppFabric.
   * @param targetLocation target location to store the downloaded file
   * @throws IOException if file downloading or writing to target location fails.
   */
  public void download(URI sourceURI, Location targetLocation) throws IOException {
    OutputStream outputStream = targetLocation.getOutputStream();
    HttpRequest request = remoteClient.requestBuilder(
      HttpMethod.GET,
      String.format("location/%s", Base64.getEncoder().encodeToString(sourceURI.toString().getBytes())))
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
          LOG.error("Failed to download file from {} to {}", sourceURI, targetLocation.toURI());
          return false;
        }
        return true;
      }

      @Override
      public void onFinished() {
        try {
        outputStream.close();
        } catch (Exception e) {
          LOG.error("Failed to close the file downloaded from {} to {}", sourceURI, targetLocation.toURI());
        }
      }
    }).build();
    HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    httpResponse.consumeContent();

    if (httpResponse.getResponseCode() != HttpResponseStatus.OK.code()) {
      throw new IOException("Failed to download file from {} to {} " + httpResponse.getResponseBodyAsString());
    }
  }
}
