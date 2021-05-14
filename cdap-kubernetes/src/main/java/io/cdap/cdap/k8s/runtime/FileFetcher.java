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

import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Download file from AppFabric via internal REST API calls.
 * <p>
 * The target file could be either residing in AppFabric's local file system or a distributed file system
 * that AppFabric is configured to access.
 */
class FileFetcher {
  static final String APP_FABRIC_HTTP = "appfabric";
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcher.class);
  DiscoveryServiceClient discoveryServiceClient;

  FileFetcher(DiscoveryServiceClient discoveryClient) {
    this.discoveryServiceClient = discoveryClient;
  }

  Discoverable pickRandom(ServiceDiscovered serviceDiscovered) {
    LOG.warn("wyzhang: serviceDiscovered = " + serviceDiscovered.toString());
    Discoverable result = null;
    Iterator<Discoverable> iter = serviceDiscovered.iterator();
    int count = 0;
    LOG.warn("wyzhang: serviceDiscovered iter start");
    while (iter.hasNext()) {
      LOG.warn("wyzhang: serviceDiscovered iter one iter");
      Discoverable next = iter.next();
      LOG.warn("wyzhang: discoverable = " + next.toString());
      if (ThreadLocalRandom.current().nextInt(++count) == 0) {
        result = next;
      }
    }
    LOG.warn("wyzhang: serviceDiscovered iter end");
    return result;
  }

  void downloadWithRetry(URI sourceURI, Location targetLocation) throws IOException, IllegalArgumentException {
    long initDelaySec = 5;
    long maxDeplySec = 30;
    long maxRetries = 5;
    int retries = 0;
    while (true) {
      try {
        LOG.warn("wyzhang: download start");
        download(sourceURI, targetLocation);
        LOG.warn("wyzhang: download succeeds");
        return;
      } catch (IOException e) {
        retries++;
        LOG.warn("wyzhang: download failed retries=" + retries);
        if (retries >= maxRetries) {
          throw e;
        }
        TimeUnit.SECONDS.toMillis(Math.min(initDelaySec * (1L << retries), maxDeplySec));
      } catch (IllegalArgumentException e) {
        LOG.warn("wyzhang: download failed exception");
        throw e;
      }
    }
  }

  /**
   * Download a file from AppFabric and store it in the target file.
   *
   * @param sourceURI uri to identity the file to download. This URI should exist in AppFabric.
   * @param targetLocation target location to store the downloaded file
   * @throws IOException if file downloading or writing to target location fails.
   */
  void download(URI sourceURI, Location targetLocation) throws IOException, IllegalArgumentException {
    LOG.warn("wyzhang: download start");
//    RemoteClient remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
//                                                 new DefaultHttpRequestConfig(false),
//                                                 Constants.Gateway.INTERNAL_API_VERSION_3);
    Discoverable discoverable = pickRandom(discoveryServiceClient.discover(APP_FABRIC_HTTP));
    if (discoverable == null) {
      throw new IOException(String.format("service %s not found by discoveryService", APP_FABRIC_HTTP));
    }
    String scheme = URIScheme.getScheme(discoverable).scheme;
    LOG.warn("wyzhang: scheme " + scheme);
    InetSocketAddress address = discoverable.getSocketAddress();
    LOG.warn("wyzhang: address " + address.toString());
    URI uri = URI.create(String.format("%s://%s:%d/%s/%s",
                                       scheme, address.getHostName(), address.getPort(),
                                       "v3Internal/location", sourceURI.getPath()));
    URL url = uri.toURL();
    LOG.warn("wyzhang: url " + url.toString());

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setReadTimeout(15000);
    conn.setConnectTimeout(15000);
    conn.setChunkedStreamingMode(0);
    LOG.warn("wyzhang: conn " + conn.toString());
    conn.connect();
    int responseCode = conn.getResponseCode();
    LOG.warn("wyzhang: resp code " + responseCode);
    if (responseCode != 200) {
      if (responseCode == 404) {
        throw new FileNotFoundException(conn.getResponseMessage());
      }
      if (responseCode == 400) {
        throw new IllegalArgumentException(conn.getResponseMessage());
      }
      throw new IOException(conn.getResponseMessage());
    }

    InputStream inputStream = conn.getInputStream();
    OutputStream outputStream = targetLocation.getOutputStream();

    byte[] buf = new byte[64 * 1024];
    int length;
    while ((length = inputStream.read(buf)) > 0) {
      outputStream.write(buf, 0, length);
    }
    outputStream.close();
  }

  enum URIScheme {
    HTTP("http", new byte[0], 80),
    HTTPS("https", "https://".getBytes(StandardCharsets.UTF_8), 443);

    private final String scheme;
    private final byte[] discoverablePayload;
    private final int defaultPort;


    URIScheme(String scheme, byte[] discoverablePayload, int defaultPort) {
      this.scheme = scheme;
      this.discoverablePayload = discoverablePayload;
      this.defaultPort = defaultPort;
    }

    public static URIScheme getScheme(Discoverable discoverable) {
      for (URIScheme scheme : values()) {
        if (scheme.isMatch(discoverable)) {
          return scheme;
        }
      }
      return HTTP;
    }

    public boolean isMatch(Discoverable discoverable) {
      return Arrays.equals(discoverablePayload, discoverable.getPayload());
    }
  }
}
