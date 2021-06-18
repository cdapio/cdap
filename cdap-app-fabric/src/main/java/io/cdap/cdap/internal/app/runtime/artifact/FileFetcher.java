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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Fetch file from {@link FileFetcherHttpHandlerInternal}
 */
class FileFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcher.class);
  private final RemoteClient remoteClient;

  FileFetcher(AuthenticationContext authenticationContext,
              DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(authenticationContext,
                                         discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Download a file from {@link FileFetcherHttpHandlerInternal}.
   *
   * @param sourceURI uri to identity the file to be downloaded.
   * @param outputStream target outpustream for the downloaded file.
   * @throws IOException if file downloading or writing to target location fails.
   */
  void download(URI sourceURI, OutputStream outputStream) throws IOException {
    LOG.warn("wyzhang: file fetcher download uri {}", sourceURI);

    HttpRequest request = remoteClient.requestBuilder(
      HttpMethod.GET,
      String.format("location/%s", sourceURI.getPath())).withContentConsumer(
      new HttpContentConsumer() {
        @Override
        public boolean onReceived(ByteBuffer chunk) {
          try {
            byte[] bytes = new byte[chunk.remaining()];
            chunk.get(bytes, 0, bytes.length);
            outputStream.write(bytes);
          } catch (IOException e) {
            LOG.error("Failed to download file {} ", sourceURI, e);
            return false;
          }
          return true;
        }

        @Override
        public void onFinished() {
        }
      }).build();
    HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    httpResponse.consumeContent();

    if (httpResponse.getResponseCode() != HttpResponseStatus.OK.code()) {
      if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
        throw new FileNotFoundException(httpResponse.getResponseBodyAsString());
      }
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
  }
}
