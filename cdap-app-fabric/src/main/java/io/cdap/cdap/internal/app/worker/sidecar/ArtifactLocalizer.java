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

  private static final String PD_DIR = "data/";
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private LocationFactory locationFactory;

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

  public Location getArtifact(ArtifactId artifactId) throws Exception {

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

    String url = String.format("test/namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                               namespaceId,
                               artifactId.getArtifact(),
                               artifactId.getVersion(),
                               scope);

    if (lastModifiedTimestamp != null) {
      url += String.format("&lastModified=%d", lastModifiedTimestamp);
    }

    Long finalLastModifiedTimestamp = lastModifiedTimestamp;
    String finalUrl = url;
    Location newLocation = Retries.callWithRetries(() -> {
      HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.GET, finalUrl);

      if (urlConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        switch (urlConn.getResponseCode()) {
          // If we get this response that means we already have the most up to date artifact
          case HttpURLConnection.HTTP_NO_CONTENT:
            urlConn.disconnect();
            return getArtifactJarLocation(artifactId, finalLastModifiedTimestamp);
          case HttpURLConnection.HTTP_NOT_FOUND:
            throw new ArtifactNotFoundException(artifactId);
            //TODO: maybe add a case for auth error?
          default:
            String err = CharStreams.toString(new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8));
            throw new IOException(
              String.format("Failed to fetch artifact %s from app-fabric due to %s", artifactId, err));
        }
      }

      Map<String, List<String>> headers = urlConn.getHeaderFields();
      if (!headers.containsKey(HttpHeaders.LAST_MODIFIED) || headers.get(HttpHeaders.LAST_MODIFIED).size() == 0) {
        //TODO figure out how to handle this
        return null;
      }

      // TODO figure out why this is a list
      Long newTimestamp = Long.valueOf(headers.get(HttpHeaders.LAST_MODIFIED).get(0));
      Location newJarLocation = getArtifactJarLocation(artifactId, newTimestamp);
      try (InputStream in = urlConn.getInputStream();
           OutputStream out = newJarLocation.getOutputStream()) {
        ByteStreams.copy(in, out);
      }
      urlConn.disconnect();
      return newJarLocation;
    }, retryStrategy);

    // This means we already have a jar but its out of date, we should delete the jar and the unpacked directory
    if (lastModifiedTimestamp != null) {
      Location oldJarLocation = getArtifactJarLocation(artifactId, lastModifiedTimestamp);
      Location oldUnpackLocation = getUnpackLocalPath(artifactId, lastModifiedTimestamp);

      try {
        oldJarLocation.delete();
        oldUnpackLocation.delete(true);
      } catch (IOException e) {
        //Catch and log the exception, this should not cause the operation to fail
        LOG.warn("Failed to delete old cached jar for artifact {} version {}: {}", artifactId.getArtifact(),
                 artifactId.getVersion(), e);
      }
    }

    return newLocation;
  }

  public Location getAndUnpackArtifact(ArtifactId artifactId) throws Exception {
    Location jarLocation = getArtifact(artifactId);
    Location unpackDir = getUnpackLocalPath(artifactId, Long.valueOf(jarLocation.getName().split("\\.")[0]));
    if (!unpackDir.exists()) {
      BundleJarUtil.unJar(jarLocation, new File(unpackDir.toURI()));
    }
    return unpackDir;
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
