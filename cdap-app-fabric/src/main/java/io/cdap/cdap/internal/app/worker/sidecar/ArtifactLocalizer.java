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

import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArtifactLocalizer is responsible for fetching, caching and unpacking artifacts requested by the
 * worker pod. The HTTP endpoints are defined in {@link ArtifactLocalizerHttpHandlerInternal}. This
 * class will run in the sidecar container that is defined by {@link
 * ArtifactLocalizerTwillRunnable}.
 *
 * <p>
 * Artifacts will be cached using the following file structure:
 * {@code /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar}
 * </p>
 *
 * <p>
 * Artifacts will be unpacked using the following file structure:
 * {@code /DATA_DIRECTORY/unpacked/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>/...}
 * </p>
 *
 * <p>
 * The procedure for fetching an artifact is:<br>
 * 1. Check if there is a locally cached version of the artifact, if so fetch the lastModified
 * timestamp from the filename.<br>
 * 2. Send a request to the
 * {@link io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal#getArtifactBytes}
 * endpoint in appfabric and provide the lastModified timestamp (if available) <br>
 * 3. If a lastModified timestamp was not specified, or there is a newer version of the artifact: appfabric will stream
 * the bytes for the newest version of the jar and pass the new lastModified timestamp in the
 * response headers <br>
 * OR <br>
 * If the provided lastModified timestamp matches the newest version of the artifact: appfabric will
 * return NOT_MODIFIED <br>
 * 4. Return the local path to the newest version of the artifact jar. <br>
 * NOTE: There is no need to invalidate the cache at any point since we will always need to call
 * appfabric to confirm that the cached version is the newest version available.
 * </p>
 */
public class ArtifactLocalizer extends AbstractArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);

  private final CConfiguration cConf;
  private final ArtifactManagerFactory artifactManagerFactory;
  private final RemoteClient remoteClient;

  /**
   * Constructor for ArtifactLocalizer.
   */
  @Inject
  public ArtifactLocalizer(CConfiguration cConf, RemoteClientFactory remoteClientFactory,
      ArtifactManagerFactory artifactManagerFactory) {
    super(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
        RetryStrategies.fromConfiguration(cConf, Constants.Service.TASK_WORKER + "."));
    this.cConf = cConf;
    this.artifactManagerFactory = artifactManagerFactory;
    // TODO (CDAP-18047) verify SSL cert should be enabled.
    this.remoteClient = remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
        RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG,
        Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching
   * the artifact as well as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @return The Local Location for this artifact
   * @throws IOException if there was an exception while fetching or caching the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getArtifact(ArtifactId artifactId) throws Exception {
    return Retries.callWithRetries(() -> fetchArtifact(artifactId), retryStrategy);
  }

  /**
   * Gets the location on the local filesystem for the directory that contains the unpacked
   * artifact. This method handles fetching, caching and unpacking the artifact.
   *
   * @param artifactId The ArtifactId of the artifact to fetch and unpack
   * @return The Local Location of the directory that contains the unpacked artifact files
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching, caching or unpacking the
   *     artifact
   * @throws Exception if there was an unexpected error
   */
  public File getAndUnpackArtifact(ArtifactId artifactId) throws Exception {
    File jarLocation = getArtifact(artifactId);
    File unpackDir = getUnpackLocalPath(artifactId,
        Long.parseLong(jarLocation.getName().split("\\.")[0]));
    if (unpackDir.exists()) {
      LOG.debug("Found unpack directory as {}", unpackDir);
      return unpackDir;
    }

    LOG.debug("Unpack directory doesn't exist, unpacking into {}", unpackDir);

    // Ensure the path leading to the unpack directory is created so we can create a temp directory
    if (!DirUtils.mkdirs(unpackDir.getParentFile())) {
      throw new IOException(
          String.format("Failed to create one or more directories along the path %s",
              unpackDir.getParentFile().getPath()));
    }

    // It is guarantee that the jarLocation is a file, not a directory, as this is the class who cache unpacked
    // artifact.
    try (ClassLoaderFolder classLoaderFolder = BundleJarUtil.prepareClassLoaderFolder(
        jarLocation, () -> DirUtils.createTempDir(unpackDir.getParentFile()))) {

      Files.move(classLoaderFolder.getDir().toPath(), unpackDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    }
    return unpackDir;
  }

  /**
   * fetchArtifact attempts to connect to app fabric to download the given artifact. This method
   * will throw {@link RetryableException} in certain circumstances.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @return The Local Location for this artifact
   * @throws IOException If an unexpected error occurs while writing the artifact to the
   *     filesystem
   * @throws ArtifactNotFoundException If the given artifact does not exist
   */
  public File fetchArtifact(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    File artifactDir = getArtifactDirLocation(artifactId);
    return fetchArtifact(artifactId, remoteClient, artifactDir);
  }

  private Path getLocalPath(String dirName, ArtifactId artifactId) {
    return Paths.get(dataDir, dirName, artifactId.getNamespace(), artifactId.getArtifact(),
        artifactId.getVersion());
  }

  /**
   * Returns a {@link File} representing the cache directory jars for the given artifact. The file
   * path is: {@literal /DATA_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/}
   */
  private File getArtifactDirLocation(ArtifactId artifactId) {
    return getLocalPath("artifacts", artifactId).toFile();
  }

  /**
   * Returns a {@link File} representing the directory containing the unpacked contents of the jar
   * for the given artifact and timestamp. The file path is:
   * {@code /DATA_DIRECTORY/unpacked/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>}
   */
  private File getUnpackLocalPath(ArtifactId artifactId, long lastModifiedTimestamp) {
    return getLocalPath("unpacked", artifactId).resolve(String.valueOf(lastModifiedTimestamp))
        .toFile();
  }

  /**
   * Preloads the latest version of selected artifacts.
   *
   * @param artifactNames list of artifact names to be preloaded in cache
   * @param versionCount number of versions to be cached
   */
  public void preloadArtifacts(Set<String> artifactNames, int versionCount)
      throws IOException {
    ArtifactManager artifactManager = artifactManagerFactory.create(
        NamespaceId.SYSTEM,
        RetryStrategies.fromConfiguration(cConf, Constants.Service.TASK_WORKER + "."));


    List<ArtifactInfo> allArtifacts = artifactManager.listArtifacts();
    for (String artifactName : artifactNames) {
      List<ArtifactInfo> artifactsToPreload = allArtifacts.stream()
          .filter(artifactInfo -> artifactInfo.getName().equals(artifactName))
          .sorted(Comparator.comparing((ArtifactInfo artifactInfo) ->
              new ArtifactVersion(artifactInfo.getVersion())).reversed())
          .limit(versionCount)
          .collect(Collectors.toList());

      if (artifactsToPreload.isEmpty()) {
          LOG.warn("Found no artifact to preload for {}", artifactName);
      }

      for (ArtifactInfo artifactInfo : artifactsToPreload) {
        LOG.info("Preloading artifact {}:{}-{}", artifactInfo.getScope(),
            artifactInfo.getName(),
            artifactInfo.getVersion());

        ArtifactId artifactId = NamespaceId.SYSTEM.artifact(artifactInfo.getName(),
            artifactInfo.getVersion());
        try {
          fetchArtifact(artifactId);
        } catch (Exception e) {
          LOG.debug("Failed to preload artifact {}", artifactId);
        }
      }
    }
  }
}
