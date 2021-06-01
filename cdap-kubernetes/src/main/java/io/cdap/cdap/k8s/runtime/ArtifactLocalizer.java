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
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);

  private final MasterEnvironmentRunnableContext context;

  private static final String PD_DIR = "data/";
  private LocationFactory locationFactory;

  public ArtifactLocalizer(MasterEnvironmentRunnableContext context,
                           @SuppressWarnings("unused") MasterEnvironment masterEnv) {
    this.context = context;
    locationFactory = new LocalLocationFactory();
  }

  //  public ArtifactLocalizer(DiscoveryServiceClient discoveryClient) {
  //
  //    Discoverable discoverable = discoveryClient.discover("appfabric").iterator().next();
  //    String scheme = "http";
  //    InetSocketAddress address = discoverable.getSocketAddress();
  //    String path = "v3Internal/location/";
  //    uri = URI.create(String.format("%s://%s:%d/%s", scheme, address.getHostName(), address.getPort(), path));
  //  }

  public Location getArtifact(Location remoteLocation) throws IOException {
    String url = "v3Internal/location/" + remoteLocation.toURI().toString();
    LOG.error("Attempting to read from URL: " + url);
    HttpURLConnection connection = context
      .openHttpURLConnection(url);

    LOG.error("Connection opened to: " + connection.getURL().toString());

    // If the location already exists then its cached and we dont need to do anything
    Location localLocation = getArtifactLocalLocation(remoteLocation);
    if (localLocation.exists()) {
      return localLocation;
    }

    try {
      try (InputStream is = connection.getInputStream()) {
        try (OutputStream os = localLocation.getOutputStream()) {
          ByteStreams.copy(is, os);
        }
        LOG.debug("Stored artifact into " + localLocation.toURI().toString());
      } catch (Exception e) {
        LOG.error("Got exception when trying to store artifact into: " + localLocation.toURI().toString(), e);
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
    try (ZipInputStream input = new ZipInputStream(new BufferedInputStream(new FileInputStream(artifactFile)))) {

      Files.createDirectories(targetPath);

      ZipEntry entry;
      while ((entry = input.getNextEntry()) != null) {

        Path output = targetPath.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(input, output);
        }
      }
    }
    return locationFactory.create(targetPath.toUri());
  }

  private Location getLocalLocation(String basePath, Location remoteLocation) {
    String path = Paths.get(PD_DIR, basePath, remoteLocation.toURI().toString()).toUri().toString();
    return locationFactory.create(path);
  }

  private Location getArtifactLocalLocation(Location remoteLocation) {
    return getLocalLocation("artifacts", remoteLocation);
  }

  private Path getUnpackLocalPath(Location remoteLocation) {
    // TODO: Fix this really gross way to get a path
    return Paths.get(getLocalLocation("unpacked", remoteLocation).toURI().toString().replace(".jar", ""));
  }
}
