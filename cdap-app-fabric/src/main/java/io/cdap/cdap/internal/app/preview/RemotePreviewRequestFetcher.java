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

package io.cdap.cdap.internal.app.preview;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Fetch preview requests from remote server.
 */
public class RemotePreviewRequestFetcher implements PreviewRequestFetcher {
  private final RemoteClient remoteClientInternal;
  private final byte[] pollerInfo;
  private static final Gson GSON = new Gson();

  public RemotePreviewRequestFetcher(DiscoveryServiceClient discoveryServiceClient, byte[] pollerInfo) {
    this.remoteClientInternal = new RemoteClient(discoveryServiceClient, Constants.Service.PREVIEW_HTTP,
                                                 new DefaultHttpRequestConfig(false),
                                                 Constants.Gateway.INTERNAL_API_VERSION_3 + "/previews");
    this.pollerInfo = pollerInfo;
  }

  @Override
  public Optional<PreviewRequest> fetch() throws IOException {
    HttpRequest.Builder builder = remoteClientInternal.requestBuilder(HttpMethod.POST, "requests/pull");
    builder.withBody(ByteBuffer.wrap(pollerInfo));
    HttpResponse httpResponse = remoteClientInternal.execute(builder.build());
    if (httpResponse.getResponseCode() == 200) {
      PreviewRequest previewRequest = GSON.fromJson(httpResponse.getResponseBodyAsString(), PreviewRequest.class);
      return Optional.ofNullable(previewRequest);
    }
    throw new IOException(String.format("Received status code:%s and body: %s", httpResponse.getResponseCode(),
                                        httpResponse.getResponseBodyAsString(Charsets.UTF_8)));
  }
}
