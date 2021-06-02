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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);

  private static final String PD_DIR = "data/";
  private final RemoteClient remoteClient;
  private LocationFactory locationFactory;

  @Inject
  public ArtifactLocalizer(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                           RemotePluginFinder remotePluginFinder) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    locationFactory = new LocalLocationFactory();
  }

  @VisibleForTesting
  public ArtifactLocalizer(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                           RemotePluginFinder remotePluginFinder,
                           File basePath) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    locationFactory = new LocalLocationFactory(basePath);
  }

  public Location getArtifact(ArtifactId artifactId) throws IOException,
    ArtifactNotFoundException, UnauthorizedException {

    Location localLocation = getArtifactLocalLocation(artifactId);
    int lastModifiedTimestamp = 0;
    // If the local cache exists, check if we have a timestamp directory
    if (localLocation.exists()) {
      String[] fileList = Paths.get(localLocation.toURI()).toFile().list();
      if (fileList != null && fileList.length > 1) {
        Integer integer = Arrays.stream(fileList).map(Integer::valueOf).max(Integer::compare).get();
      }
    }

    String namespaceId = artifactId.getNamespace();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (ArtifactScope.SYSTEM.toString().equalsIgnoreCase(namespaceId)) {
      namespaceId = "default";
      scope = ArtifactScope.SYSTEM;
    }
    String url = String.format("test/namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                               namespaceId,
                               artifactId.getArtifact(),
                               artifactId.getVersion(),
                               scope);

    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.GET, url);

    return null;
  }

  //  public Location getArtifact(Location remoteLocation) throws IOException {
  //
  //    // If the location already exists then its cached and we dont need to do anything
  //    Location localLocation = getArtifactLocalLocation(remoteLocation);
  //    if (localLocation.exists()) {
  //      return localLocation;
  //    }
  //
  //    String url = "location" + remoteLocation.toString();
  //    HttpURLConnection connection = remoteClient.openConnection(HttpMethod.GET, url);
  //    LOG.error("Connection opened to: " + connection.getURL().toString());
  //    try {
  //      try (InputStream is = connection.getInputStream()) {
  //        try (OutputStream os = localLocation.getOutputStream()) {
  //          ByteStreams.copy(is, os);
  //        }
  //        LOG.debug("Stored artifact into " + localLocation.toString());
  //      } catch (Exception e) {
  //        LOG.error("Got exception when trying to store artifact into: " + localLocation.toString(), e);
  //        // Just treat bad request as IOException since it won't be retriable
  //        throw new IOException(e);
  //      }
  //    } finally {
  //      connection.disconnect();
  //    }
  //
  //    return localLocation;
  //  }

  //  public Location getAndUnpackArtifact(Location remoteLocation) throws Exception {
  //    Location artifactLocation = getArtifact(remoteLocation);
  //
  //    File artifactFile = Paths.get(artifactLocation.toURI()).toFile();
  //    Path targetPath = getUnpackLocalPath(remoteLocation);
  //    BundleJarUtil.unJar(artifactFile, targetPath.toFile());
  //    return locationFactory.create(targetPath.toUri());
  //  }

  private Location getLocalLocation(String basePath, ArtifactId artifactId) {
    String path = Paths.get(PD_DIR, basePath, artifactId.getNamespace(), artifactId.getArtifact(),
                            artifactId.getVersion()).toString();
    return locationFactory.create(path);
  }

  private Location getArtifactLocalLocation(ArtifactId artifactId) {
    return getLocalLocation("artifacts", artifactId);
  }

  private Path getUnpackLocalPath(ArtifactId artifactId) {
    // TODO: Fix this really gross way to get a path
    return Paths.get(getLocalLocation("unpacked", artifactId)
                       .toString()
                       .replace(".jar", ""));
  }
}
