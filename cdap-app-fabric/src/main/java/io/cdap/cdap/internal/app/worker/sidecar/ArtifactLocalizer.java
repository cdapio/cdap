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
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);


  private static final String PD_DIR = "data/";
  private final RemoteClient remoteClient;
  private LocationFactory locationFactory;

  @Inject
  public ArtifactLocalizer(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                                       HttpRequestConfig.DEFAULT,
                                                       Constants.Gateway.INTERNAL_API_VERSION_3);
    locationFactory = new LocalLocationFactory();
  }

  @VisibleForTesting
  public ArtifactLocalizer(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient, File basePath) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    locationFactory = new LocalLocationFactory(basePath);
  }

  public Location getArtifact(Location remoteLocation) throws IOException {


    // If the location already exists then its cached and we dont need to do anything
    Location localLocation = getArtifactLocalLocation(remoteLocation);
    if (localLocation.exists()) {
      return localLocation;
    }

    String url = "location" + remoteLocation.toString();
    HttpURLConnection connection = remoteClient.openConnection(HttpMethod.GET, url);
    LOG.error("Connection opened to: " + connection.getURL().toString());
    try {
      try (InputStream is = connection.getInputStream()) {
        try (OutputStream os = localLocation.getOutputStream()) {
          ByteStreams.copy(is, os);
        }
        LOG.debug("Stored artifact into " + localLocation.toString());
      } catch (Exception e) {
        LOG.error("Got exception when trying to store artifact into: " + localLocation.toString(), e);
        // Just treat bad request as IOException since it won't be retriable
        throw new IOException(e);
      }
    } finally {
      connection.disconnect();
    }

    return localLocation;
  }

  public Location getAndUnpackArtifact(Location remoteLocation) throws Exception {
    Location artifactLocation = getArtifact(remoteLocation);

    File artifactFile = Paths.get(artifactLocation.toURI()).toFile();
    Path targetPath = getUnpackLocalPath(remoteLocation);
    BundleJarUtil.unJar(artifactFile, targetPath.toFile());
    return locationFactory.create(targetPath.toUri());
  }

  private Location getLocalLocation(String basePath, Location remoteLocation) {
    String path = Paths.get(PD_DIR, basePath, remoteLocation.toString()).toString();
    return locationFactory.create(path);
  }

  private Location getArtifactLocalLocation(Location remoteLocation) {
    return getLocalLocation("artifacts", remoteLocation);
  }

  private Path getUnpackLocalPath(Location remoteLocation) {
    // TODO: Fix this really gross way to get a path
    return Paths.get(getLocalLocation("unpacked", remoteLocation).toString().replace(".jar", ""));
  }
}
