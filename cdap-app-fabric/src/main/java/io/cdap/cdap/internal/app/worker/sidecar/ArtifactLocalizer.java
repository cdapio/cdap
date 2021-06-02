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
import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.id.ArtifactId;
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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);

  // TODO: Move this directory name into a cConf property
  private static final String PD_DIR = "data/";
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private final LocationFactory locationFactory;

  @Inject
  public ArtifactLocalizer(DiscoveryServiceClient discoveryServiceClient, LocationFactory locationFactory) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    this.locationFactory = locationFactory;
    this.retryStrategy = RetryStrategies.exponentialDelay(100, 1000, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public ArtifactLocalizer(DiscoveryServiceClient discoveryServiceClient, File basePath) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    locationFactory = new LocalLocationFactory(basePath);
    this.retryStrategy = RetryStrategies.exponentialDelay(100, 1000, TimeUnit.MILLISECONDS);
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching the artifact as well
   * as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @return The Local Location for this artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching or caching the artifact
   * @throws Exception if there was an unexpected error
   */
  public Location getArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Fetching artifact info for {}", artifactId);
    Location localLocation = getArtifactDirLocation(artifactId);
    Long lastModifiedTimestamp = null;
    // If the local cache exists, check if we have a timestamp directory
    if (localLocation.exists()) {
      String[] fileList = Paths.get(localLocation.toURI()).toFile().list();
      if (fileList != null && fileList.length > 0) {
        //Split on period to get timestamp without the .jar extension
        lastModifiedTimestamp = Arrays.stream(fileList).map(s -> Long.valueOf(s.split("\\.")[0]))
          .max(Long::compare).get();
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

    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                               namespaceId,
                               artifactId.getArtifact(),
                               artifactId.getVersion(),
                               scope);

    if (lastModifiedTimestamp != null) {
      LOG.debug("Found existing local version with timestamp {}", lastModifiedTimestamp);
      url += String.format("&lastModified=%d", lastModifiedTimestamp);
    }

    Long finalLastModifiedTimestamp = lastModifiedTimestamp;
    String finalUrl = url;
    Location newJarLocation = Retries
      .callWithRetries(() -> fetchArtifact(artifactId, finalUrl, finalLastModifiedTimestamp), retryStrategy);

    // If we didn't have a version that was previous cached then we're done and we can return the location
    if (lastModifiedTimestamp == null) {
      return newJarLocation;
    }

    // This means we already have a jar but its out of date, we should delete the jar and the unpacked directory
    Location oldJarLocation = getArtifactJarLocation(artifactId, lastModifiedTimestamp);
    LOG.debug("Got new location as {} with old location as {}", newJarLocation, oldJarLocation);
    if (!newJarLocation.equals(oldJarLocation)) {
      Location oldUnpackLocation = getUnpackLocalPath(artifactId, lastModifiedTimestamp);
      LOG.debug("Deleting previously cached jar");
      try {
        oldJarLocation.delete();
        oldUnpackLocation.delete(true);
      } catch (IOException e) {
        //Catch and log the exception, this should not cause the operation to fail
        LOG.warn("Failed to delete old cached jar for artifact {} version {}: {}", artifactId.getArtifact(),
                 artifactId.getVersion(), e);
      }
    }
    return newJarLocation;
  }

  /**
   * Gets the location on the local filesystem for the directory that contains the unpacked artifact. This method
   * handles fetching, caching and unpacking the artifact.
   *
   * @param artifactId The ArtifactId of the artifact to fetch and unpack
   * @return The Local Location of the directory that contains the unpacked artifact files
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching, caching or unpacking the artifact
   * @throws Exception if there was an unexpected error
   */
  public Location getAndUnpackArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Unpacking artifact {}", artifactId);
    Location jarLocation = getArtifact(artifactId);
    Location unpackDir = getUnpackLocalPath(artifactId, Long.valueOf(jarLocation.getName().split("\\.")[0]));
    LOG.debug("Got unpack directory as {}", unpackDir);
    if (!unpackDir.exists()) {
      LOG.debug("Unpack directory doesnt exist, creating it now");
      BundleJarUtil.unJar(jarLocation, new File(unpackDir.toURI()));
    }
    return unpackDir;
  }

  private Location fetchArtifact(ArtifactId artifactId, String url,
                                 Long lastModifiedTimestamp) throws IOException, ArtifactNotFoundException {
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.GET, url);

    if (urlConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      switch (urlConn.getResponseCode()) {
        // If we get this response that means we already have the most up to date artifact
        case HttpURLConnection.HTTP_NO_CONTENT:
          LOG.debug("Call to app fabric returned NO_CONTENT");
          urlConn.disconnect();
          return getArtifactJarLocation(artifactId, lastModifiedTimestamp);
        case HttpURLConnection.HTTP_NOT_FOUND:
          throw new ArtifactNotFoundException(artifactId);
        default:
          String err = CharStreams.toString(new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8));
          throw new IOException(
            String.format("Failed to fetch artifact %s from app-fabric due to %s", artifactId, err));
      }
    }

    Map<String, List<String>> headers = urlConn.getHeaderFields();
    if (!headers.containsKey(HttpHeaders.LAST_MODIFIED) || headers.get(HttpHeaders.LAST_MODIFIED).size() != 1) {
      //Not sure how this would since this endpoint should always set the header.
      // If it does happen we should retry.
      throw new RetryableException(String
                                     .format("The response from %s did not contain the %s header.", url,
                                             HttpHeaders.LAST_MODIFIED));
    }

    Long newTimestamp = Long.valueOf(headers.get(HttpHeaders.LAST_MODIFIED).get(0));
    Location newLocation = getArtifactJarLocation(artifactId, newTimestamp);
    try (InputStream in = urlConn.getInputStream();
         OutputStream out = newLocation.getOutputStream()) {
      ByteStreams.copy(in, out);
    }
    urlConn.disconnect();
    return newLocation;
  }

  private String getLocalPath(String basePath, ArtifactId artifactId) {
    return Paths.get(PD_DIR, basePath, artifactId.getNamespace(), artifactId.getArtifact(),
                     artifactId.getVersion()).toString();
  }

  private Location getArtifactDirLocation(ArtifactId artifactId) {
    return locationFactory.create(getLocalPath("artifacts", artifactId));
  }

  private Location getArtifactJarLocation(ArtifactId artifactId, @NotNull Long lastModifiedTimestamp) {
    Path path = Paths
      .get(getLocalPath("artifacts", artifactId), String.format("%d.jar", lastModifiedTimestamp));
    return locationFactory.create(path.toString());
  }

  private Location getUnpackLocalPath(ArtifactId artifactId, @NotNull Long lastModifiedTimestamp) {
    Path path = Paths.get(getLocalPath("unpacked", artifactId), lastModifiedTimestamp.toString());
    return locationFactory.create(path.toString());
  }
}
