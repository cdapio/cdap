/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Artifact Localizer and Artifact Cache. Provides functionality to fetch and cache
 * artifacts from a remote endpoint.
 */
public abstract class AbstractArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractArtifactLocalizer.class);

  protected final String dataDir;
  protected final RetryStrategy retryStrategy;

  protected AbstractArtifactLocalizer(String dataDir, RetryStrategy retryStrategy) {
    this.dataDir = dataDir;
    this.retryStrategy = retryStrategy;
  }

  /**
   * fetchArtifact attempts to connect to app fabric to download the given artifact. This method
   * will throw {@link RetryableException} in certain circumstances.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @param remoteClient The remote client used to connect to appfabric on the peer
   * @param artifactDir The directory where the artifact will be stored
   * @return The Local Location for this artifact
   * @throws IOException If an unexpected error occurs while writing the artifact to the
   *     filesystem
   * @throws ArtifactNotFoundException If the given artifact does not exist
   */
  protected File fetchArtifact(ArtifactId artifactId, RemoteClient remoteClient, File artifactDir)
      throws IOException, ArtifactNotFoundException {
    Long lastModifiedTimestamp = getCurrentLastModifiedTimestamp(artifactDir);
    HttpURLConnection urlConn = openConnection(artifactId, remoteClient);

    try {
      if (lastModifiedTimestamp != null) {
        LOG.debug("Found existing local version for {} with timestamp {}", artifactId,
            lastModifiedTimestamp);

        ZonedDateTime lastModifiedDate = ZonedDateTime
            .ofInstant(Instant.ofEpochMilli(lastModifiedTimestamp), ZoneId.of("GMT"));
        urlConn.setRequestProperty(HttpHeaders.IF_MODIFIED_SINCE, lastModifiedDate.format(
            DateTimeFormatter.RFC_1123_DATE_TIME));
      }

      // If we get this response that means we already have the most up to date artifact
      if (lastModifiedTimestamp != null
          && urlConn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
        LOG.debug(
            "Call to app fabric returned NOT_MODIFIED for {} with lastModifiedTimestamp of {}",
            artifactId,
            lastModifiedTimestamp);
        File artifactJarLocation = getArtifactJarLocation(artifactDir, lastModifiedTimestamp);
        if (!artifactJarLocation.exists()) {
          throw new RetryableException(
              String.format("Locally cached artifact jar for %s is missing.",
                  artifactId));
        }
        return artifactJarLocation;
      }

      throwIfError(urlConn, artifactId);

      ZonedDateTime newModifiedDate = getLastModifiedHeader(urlConn);
      long newTimestamp = newModifiedDate.toInstant().toEpochMilli();
      File newLocation = getArtifactJarLocation(artifactDir, newTimestamp);
      DirUtils.mkdirs(newLocation.getParentFile());

      // Download the artifact to a temporary file then atomically rename it to the final name to
      // avoid race conditions with multiple threads.
      Path tempFile = Files.createTempFile(newLocation.getParentFile().toPath(),
          String.valueOf(newTimestamp), ".tmp");
      long start = System.currentTimeMillis();
      File downloadedFile = downloadArtifact(urlConn, newLocation.toPath(), tempFile);
      long downloadTime = (System.currentTimeMillis() - start) / 1000;
      LOG.info("Downloaded {} to location {} in {} seconds", artifactId, downloadedFile,
          downloadTime);
      return downloadedFile;
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Returns a {@link File} representing the cached jar for the given artifact and timestamp. The
   * file path is: /<artifact_dir>/<last-modified-timestamp>.jar
   */
  private File getArtifactJarLocation(File artifactDir, long lastModifiedTimestamp) {
    return artifactDir.toPath().resolve(String.format("%d.jar", lastModifiedTimestamp)).toFile();
  }

  /**
   * Opens a connection to appfabric to fetch an artifact.
   *
   * @param artifactId the ArtifactId of the artifact to fetch
   * @param remoteClient the remote client used to fetch the artifact
   * @return the HttpURLConnection
   * @throws IOException if there was an unexpected error
   */
  private HttpURLConnection openConnection(ArtifactId artifactId, RemoteClient remoteClient)
      throws IOException {
    String namespaceId = artifactId.getNamespace();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (ArtifactScope.SYSTEM.toString().equalsIgnoreCase(namespaceId)) {
      namespaceId = NamespaceId.DEFAULT.getEntityName();
      scope = ArtifactScope.SYSTEM;
    }
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
        namespaceId,
        artifactId.getArtifact(),
        artifactId.getVersion(),
        scope);
    return remoteClient.openConnection(HttpMethod.GET, url);
  }

  /**
   * Downloads an artifact using the provided {@link HttpURLConnection} and returns the location of
   * the downloaded artifact.
   *
   * @param urlConn the HttpURLConnection
   * @param destination the path where the artifact should be downloaded to
   * @param tempFile the path to temporary file used during artifact download
   * @return the location of the downloaded artifact
   * @throws IOException if there was an unexpected error
   */
  private File downloadArtifact(HttpURLConnection urlConn, Path destination, Path tempFile)
      throws IOException {
    try (InputStream in = urlConn.getInputStream()) {
      Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
      Files.move(tempFile, destination, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(tempFile);
    }
    return destination.toFile();
  }

  /**
   * This checks the local cache for this artifact and retrieves the timestamp for the newest cache
   * entry, if this artifact is not cached it returns null
   *
   * @param artifactDir the directory containing artifacts
   * @return timestamp of the newest cache entry if it exists, null otherwise
   */
  @Nullable
  private Long getCurrentLastModifiedTimestamp(File artifactDir) {
    // Check if we have cached jars in the artifact directory, if so return the latest modified timestamp.
    return DirUtils.listFiles(artifactDir, File::isFile).stream()
        .map(File::getName)
        // filter out temporary files whose name doesn't end in .jar
        .filter(name -> name.endsWith(".jar"))
        .map(FileUtils::getNameWithoutExtension)
        .map(Long::valueOf)
        .max(Long::compare)
        .orElse(null);
  }

  /**
   * Helper function for verifying, extracting and converting the Last-Modified header from the URL
   * connection.
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
      throw new RetryableException(
          String.format("The response from %s did not contain the %s header.",
              urlConn.getURL(), HttpHeaders.LAST_MODIFIED));
    }

    return lastModified;
  }

  /**
   * Helper method for catching and throwing any errors that might have occurred when attempting to
   * connect.
   */
  private void throwIfError(HttpURLConnection urlConn, ArtifactId artifactId)
      throws IOException, ArtifactNotFoundException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }

    try (Reader reader = new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8)) {
      String errMsg = CharStreams.toString(reader);
      switch (responseCode) {
        case HttpURLConnection.HTTP_NOT_FOUND:
          throw new ArtifactNotFoundException(artifactId);
        case HttpURLConnection.HTTP_UNAVAILABLE:
          throw new ServiceUnavailableException(Constants.Service.APP_FABRIC_HTTP, errMsg);
      }
      throw new IOException(
          String.format("Failed to fetch artifact %s from app-fabric due to %s", artifactId,
              errMsg));
    }
  }
}
