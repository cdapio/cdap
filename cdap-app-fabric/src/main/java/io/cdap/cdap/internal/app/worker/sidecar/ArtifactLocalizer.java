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
import com.google.common.io.CharStreams;
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
import org.apache.commons.io.FileUtils;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.resteasy.util.HttpHeaderNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

/**
 * ArtifactLocalizer is responsible for fetching, caching and unpacking artifacts requested by the worker pod. The HTTP
 * endpoints are defined in {@link ArtifactLocalizerHttpHandlerInternal}. This class will run in the sidecar container
 * that is defined by {@link ArtifactLocalizerTwillRunnable}.
 */
public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);

  // TODO: Move this directory name into a cConf property
  private static final String PD_DIR = "data/";
  public static final String LAST_MODIFIED_HEADER = HttpHeaderNames.LAST_MODIFIED.toLowerCase();
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private String basePath;

  @Inject
  public ArtifactLocalizer(DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    this.retryStrategy = RetryStrategies.exponentialDelay(100, 1000, TimeUnit.MILLISECONDS);
    this.basePath = "";
  }

  @VisibleForTesting
  public ArtifactLocalizer(DiscoveryServiceClient discoveryServiceClient, String basePath) {
    this(discoveryServiceClient);
    this.basePath = basePath;
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
  public File getArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Fetching artifact info for {}", artifactId);
    File localLocation = getArtifactDirLocation(artifactId);
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

    Long finalLastModifiedTimestamp = lastModifiedTimestamp;
    File newJarLocation = Retries
      .callWithRetries(() -> fetchArtifact(artifactId, url, finalLastModifiedTimestamp), retryStrategy);

    // If the lastModifiedTimestamp is null then there is no previous cache to delete and we can just return
    if(lastModifiedTimestamp == null){
      return newJarLocation;
    }

    // TODO (CDAP-18051): Migrate this cleanup task to its own service
    // This means we already have a jar but its out of date, we should delete the jar and the unpacked directory
    File oldJarLocation = getArtifactJarLocation(artifactId, lastModifiedTimestamp);
    LOG.debug("Got new location as {} with old location as {}", newJarLocation, oldJarLocation);
    if (!newJarLocation.equals(oldJarLocation)) {
      File oldUnpackLocation = getUnpackLocalPath(artifactId, lastModifiedTimestamp);
      LOG.debug("Deleting previously cached jar");
      try {
        oldJarLocation.delete();
        FileUtils.deleteDirectory(oldUnpackLocation);
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
  public File getAndUnpackArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Unpacking artifact {}", artifactId);
    File jarLocation = getArtifact(artifactId);
    File unpackDir = getUnpackLocalPath(artifactId, Long.valueOf(jarLocation.getName().split("\\.")[0]));
    LOG.debug("Got unpack directory as {}", unpackDir);
    if (!unpackDir.exists()) {
      LOG.debug("Unpack directory doesnt exist, creating it now");
      BundleJarUtil.unJar(jarLocation, unpackDir);
    }
    return unpackDir;
  }

  private File fetchArtifact(ArtifactId artifactId, String url,
                             Long lastModifiedTimestamp) throws IOException, ArtifactNotFoundException {
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.GET, url);
    if (lastModifiedTimestamp != null) {
      LOG.debug("Found existing local version with timestamp {}", lastModifiedTimestamp);
      urlConn.setRequestProperty(HttpHeaderNames.IF_MODIFIED_SINCE, String.valueOf(lastModifiedTimestamp));
    }

    if (urlConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      switch (urlConn.getResponseCode()) {
        // If we get this response that means we already have the most up to date artifact
        case HttpURLConnection.HTTP_NOT_MODIFIED:
          LOG.debug("Call to app fabric returned NOT_MODIFIED");
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
    if (!headers.containsKey(LAST_MODIFIED_HEADER) ||
      headers.get(LAST_MODIFIED_HEADER).size() != 1) {
      //Not sure how this would happen since this endpoint should always set the header.
      // If it does happen we should retry.
      throw new RetryableException(String
                                     .format("The response from %s did not contain the %s header.", url,
                                             LAST_MODIFIED_HEADER));
    }

    Long newTimestamp = Long.valueOf(headers.get(LAST_MODIFIED_HEADER).get(0));
    File newLocation = getArtifactJarLocation(artifactId, newTimestamp);

    try (InputStream in = urlConn.getInputStream()) {
      //      boolean mkdirs = newLocation.mkdirs();
      //      boolean newFile = newLocation.createNewFile();
      FileUtils.copyInputStreamToFile(in, newLocation);
      //      ByteStreams.copy(in, out);
    }
    urlConn.disconnect();
    return newLocation;
  }

  private Path getLocalPath(String dirName, ArtifactId artifactId) {
    return Paths.get(basePath, PD_DIR, dirName, artifactId.getNamespace(), artifactId.getArtifact(),
                     artifactId.getVersion());
  }

  private File getArtifactDirLocation(ArtifactId artifactId) {
    return getLocalPath("artifacts", artifactId).toFile();
  }

  private File getArtifactJarLocation(ArtifactId artifactId, @NotNull Long lastModifiedTimestamp) {
    return getLocalPath("artifacts", artifactId).resolve(String.format("%d.jar", lastModifiedTimestamp)).toFile();
  }

  private File getUnpackLocalPath(ArtifactId artifactId, @NotNull Long lastModifiedTimestamp) {
    return getLocalPath("unpacked", artifactId).resolve(lastModifiedTimestamp.toString()).toFile();
  }
}
