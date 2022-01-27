/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.proto.id.ArtifactId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class for Artifact Localizer and Artifact Cache.
 * Provides functionality to fetch and cache artifacts from a remote endpoint.
 */
public abstract class ArtifactLocalizerBase {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerBase.class);
  private final String dataDir;
  private final RetryStrategy retryStrategy;

  public ArtifactLocalizerBase(String dataDir, RetryStrategy retryStrategy) {
    this.dataDir = dataDir;
    this.retryStrategy = retryStrategy;
  }

  public abstract HttpURLConnection openConnection(ArtifactId artifactId, @Nullable String peer) throws Exception;

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
    return Retries.callWithRetries(() -> fetchArtifact(artifactId, null), retryStrategy);
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching the artifact as well
   * as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @param peer The tethered peer
   * @return The Local Location for this artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching or caching the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getArtifact(ArtifactId artifactId, String peer) throws Exception {
    return Retries.callWithRetries(() -> fetchArtifact(artifactId, peer), retryStrategy);
  }

  /**
   * fetchArtifact attempts to connect to app fabric to download the given artifact. This method will throw {@link
   * RetryableException} in certain circumstances so using this with the
   *
   * @param artifactId the id of the artifact to fetch
   * @return a File pointing to the locally cached jar of the newest version
   * @throws IOException If an unexpected error occurs while writing the artifact to the filesystem
   * @throws ArtifactNotFoundException If the given artifact does not exist
   */
  public File fetchArtifact(ArtifactId artifactId) throws Exception {
    return fetchArtifact(artifactId, null);
  }

  /**
   * fetchArtifact attempts to connect to app fabric to download the given artifact. This method will throw {@link
   * RetryableException} in certain circumstances so using this with the
   *
   * @param artifactId the id of the artifact to fetch
   * @param peer the tethered peer
   * @return a File pointing to the locally cached jar of the newest version
   * @throws IOException If an unexpected error occurs while writing the artifact to the filesystem
   * @throws ArtifactNotFoundException If the given artifact does not exist
   */
  public File fetchArtifact(ArtifactId artifactId, @Nullable String peer) throws Exception {
    Long lastModifiedTimestamp = getCurrentLastModifiedTimestamp(artifactId, peer);
    HttpURLConnection urlConn = openConnection(artifactId, peer);
    try {
      if (lastModifiedTimestamp != null) {
        LOG.debug("Found existing local version for {} with timestamp {}", artifactId, lastModifiedTimestamp);

        ZonedDateTime lastModifiedDate = ZonedDateTime
          .ofInstant(Instant.ofEpochMilli(lastModifiedTimestamp), ZoneId.of("GMT"));
        urlConn.setRequestProperty(HttpHeaders.IF_MODIFIED_SINCE, lastModifiedDate.format(
          DateTimeFormatter.RFC_1123_DATE_TIME));
      }

      // If we get this response that means we already have the most up to date artifact
      if (lastModifiedTimestamp != null && urlConn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
        LOG.debug("Call to app fabric returned NOT_MODIFIED for {} with lastModifiedTimestamp of {}", artifactId,
                  lastModifiedTimestamp);
        File artifactJarLocation = getArtifactJarLocation(artifactId, lastModifiedTimestamp, peer);
        if (!artifactJarLocation.exists()) {
          throw new RetryableException(String.format("Locally cached artifact jar for %s is missing.",
                                                     artifactId));
        }
        return artifactJarLocation;
      }

      throwIfError(urlConn, artifactId);

      ZonedDateTime newModifiedDate = getLastModifiedHeader(urlConn);
      long newTimestamp = newModifiedDate.toInstant().toEpochMilli();
      File newLocation = getArtifactJarLocation(artifactId, newTimestamp, peer);
      DirUtils.mkdirs(newLocation.getParentFile());

      // Download the artifact to a temporary file then atomically rename it to the final name to
      // avoid race conditions with multiple threads.
      Path tempFile = Files.createTempFile(newLocation.getParentFile().toPath(), String.valueOf(newTimestamp), ".jar");
      try (InputStream in = urlConn.getInputStream()) {
        Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tempFile, newLocation.toPath(), StandardCopyOption.ATOMIC_MOVE,
                   StandardCopyOption.REPLACE_EXISTING);
      } finally {
        Files.deleteIfExists(tempFile);
      }

      return newLocation;
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * This checks the local cache for this artifact and retrieves the timestamp for the newest cache entry, if this
   * artifact is not cached it returns null
   */
  @Nullable
  private Long getCurrentLastModifiedTimestamp(ArtifactId artifactId, @Nullable String peerName) {
    File artifactDir = peerName != null ? getArtifactDirLocation(artifactId, peerName) :
      getArtifactDirLocation(artifactId);

    // Check if we have cached jars in the artifact directory, if so return the latest modified timestamp.
    return DirUtils.listFiles(artifactDir, File::isFile).stream()
      .map(File::getName)
      .map(FileUtils::getNameWithoutExtension)
      .map(Long::valueOf)
      .max(Long::compare)
      .orElse(null);
  }

  /**
   * Helper function for verifying, extracting and converting the Last-Modified header from the URL connection.
   */
  private ZonedDateTime getLastModifiedHeader(HttpURLConnection urlConn) {
    Map<String, List<String>> headers = urlConn.getHeaderFields();
    ZonedDateTime lastModified = headers.entrySet().stream()
      .filter(headerEntry -> HttpHeaders.LAST_MODIFIED.equalsIgnoreCase(headerEntry.getKey()))
      .map(Map.Entry::getValue)
      .flatMap(Collection::stream)
      .findFirst()
      .map(s -> ZonedDateTime.parse(s, DateTimeFormatter.RFC_1123_DATE_TIME))
      .orElse(null);

    if (lastModified == null) {
      // This should never happen since this endpoint should always set the header.
      // If it does happen we should retry.
      throw new RetryableException(String.format("The response from %s did not contain the %s header.",
                                                 urlConn.getURL(), HttpHeaders.LAST_MODIFIED));
    }

    return lastModified;
  }

  /**
   * Helper method for catching and throwing any errors that might have occurred when attempting to connect.
   */
  private void throwIfError(HttpURLConnection urlConn, ArtifactId artifactId)
    throws IOException, ArtifactNotFoundException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }

    String errMsg = CharStreams.toString(new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8));
    switch (responseCode) {
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new ArtifactNotFoundException(artifactId);
      case HttpURLConnection.HTTP_UNAVAILABLE:
        throw new ServiceUnavailableException(Constants.Service.APP_FABRIC_HTTP, errMsg);
    }
    throw new IOException(
      String.format("Failed to fetch artifact %s from app-fabric due to %s", artifactId, errMsg));
  }

  Path getLocalPath(String dirName, ArtifactId artifactId) {
    return Paths.get(dataDir, dirName, artifactId.getNamespace(), artifactId.getArtifact(),
                     artifactId.getVersion());
  }

  Path getLocalPath(String peerName, String dirName, ArtifactId artifactId) {
      return Paths.get(dataDir, "peers", peerName,dirName, artifactId.getNamespace(),
                       artifactId.getArtifact(), artifactId.getVersion());
  }

  /**
   * Returns a {@link File} representing the cache directory jars for the given artifact. The file path is:
   * /DATA_DIRECTORY/peers/<peer-name>/</peer-name>artifacts/<namespace>/<artifact-name>/<artifact-version>/
   */
  private File getArtifactDirLocation(ArtifactId artifactId, String peerName) {
    return getLocalPath(peerName, "artifacts", artifactId).toFile();
  }

  /**
   * Returns a {@link File} representing the cache directory jars for the given artifact. The file path is:
   * /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/
   */
  private File getArtifactDirLocation(ArtifactId artifactId) {
    return getLocalPath("artifacts", artifactId).toFile();
  }

  /**
   * Returns a {@link File} representing the cached jar for the given artifact and timestamp. The file path is:
   * /DATA_DIRECTORY/peers/<peer-name></peer-name>/artifacts/<namespace>/<artifact-name>/<artifact-version>/
   * <last-modified-timestamp>.jar if peerName is not null,
   * /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar otherwise
   */
  private File getArtifactJarLocation(ArtifactId artifactId, long lastModifiedTimestamp, @Nullable String peerName) {
    if (peerName != null) {
      return getLocalPath(peerName, "artifacts", artifactId)
        .resolve(String.format("%d.jar", lastModifiedTimestamp)).toFile();
    }
    return getLocalPath("artifacts", artifactId)
      .resolve(String.format("%d.jar", lastModifiedTimestamp)).toFile();
  }

}
