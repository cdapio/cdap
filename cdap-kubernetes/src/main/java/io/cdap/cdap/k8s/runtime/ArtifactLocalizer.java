/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.k8s.runtime;

import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;

public class ArtifactLocalizer {
  private final URI uri;
  private static final Logger LOG = LoggerFactory.getLogger(FileLocalizer.class);

  public ArtifactLocalizer(DiscoveryServiceClient discoveryClient) {

    Discoverable discoverable = discoveryClient.discover("appfabric").iterator().next();
    String scheme = "http";
    InetSocketAddress address = discoverable.getSocketAddress();
    String path = "v3Internal/location/";
    uri = URI.create(String.format("%s://%s:%d/%s", scheme, address.getHostName(), address.getPort(), path));
  }

  private void getAndStoreArtifact(Location remoteLocation) throws IOException {
    URL url = uri.resolve(remoteLocation.toURI()).toURL();
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//    urlConn.setConnectTimeout(httpRequestConfig.getConnectTimeout());
//    urlConn.setReadTimeout(httpRequestConfig.getReadTimeout());
    connection.setDoInput(true);

    try {
      try (InputStream is = connection.getInputStream()) {
        try (OutputStream os = location.getOutputStream()) {
          ByteStreams.copy(is, os);
        }
        LOG.debug("Stored artifact into " + location.toURI().toString());
      } catch (BadRequestException e) {
        // Just treat bad request as IOException since it won't be retriable
        throw new IOException(e);
      }
    } finally {
      connection.disconnect();
    }
  }

}
