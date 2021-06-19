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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.worker.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Perform artifact inspection remotely in {@link TaskWorkerHttpHandlerInternal}
 */
public class RemoteArtifactInspector implements ArtifactInspector {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteArtifactInspector.class);

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final RemoteTaskExecutor remoteTaskExecutor;

  RemoteArtifactInspector(CConfiguration cConf,
                          LocationFactory locationFactory,
                          RemoteClientFactory remoteClientFactory) {
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, remoteClientFactory);
  }

  @Override
  public ArtifactClassesWithMetadata inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                                     @Nullable ClassLoader parentClassloader,
                                                     @Nullable List<ArtifactDetail> parentArtifacts,
                                                     Set<PluginClass> additionalPlugins)
    throws IOException, InvalidArtifactException {

    LOG.warn("wyzhang: inspect artifact {} file {}", artifactId, artifactFile);

    Location artifactLocation = uploadFile(artifactId, artifactFile);

    RemoteArtifactInspectTaskRequest req = new RemoteArtifactInspectTaskRequest(artifactLocation.toURI(),
                                                                                parentArtifacts,
                                                                                additionalPlugins);
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(RemoteArtifactInspectTask.class.getName())
        .withNamespace(artifactId.getNamespace().getId())
        .withArtifact(artifactId.toArtifactId())
        .withParam(GSON.toJson(req, RemoteArtifactInspectTaskRequest.class))
        .build();

      byte[] result = remoteTaskExecutor.runTask(request);
      return GSON.fromJson(new String(result, StandardCharsets.UTF_8),
                           ArtifactClassesWithMetadata.class);
    } catch (RemoteExecutionException e) {
      LOG.error("wyzhang:", e);
      if (e.getCause().getRemoteExceptionClassName().equals(InvalidArtifactException.class.getName())) {
        throw new InvalidArtifactException(e.getMessage(), e);
      }
      if (e.getCause().getRemoteExceptionClassName().equals(IOException.class.getName())) {
        throw new IOException(e.getMessage(), e);
      }

      LOG.error("wyzhang: remote artifact inspection exception : {}", e.getCause().getRemoteExceptionClassName());
    } catch (Exception e) {
      throw new IOException("Failed to inspect artifact : " + e.getMessage(), e);
    }

    return null;
  }

  /**
   * upload jar to a tmp {@link Location}.
   */
  private Location uploadFile(Id.Artifact artifactId, File artifactFile) throws IOException {

    String path = String.format("%s/%s/%s-%s-%d", cConf.get(Constants.AppFabric.TEMP_DIR),
                                artifactId.getNamespace().getId(),
                                artifactId.getName(),
                                artifactId.getVersion().toString(),
                                artifactFile.lastModified());

    Location tmpLocation = locationFactory.create(path);

    LOG.debug("Copy from local {} to {}", artifactFile, tmpLocation);

    LOG.warn("wyzhang: Copy from local {} to {}", artifactFile, tmpLocation);

    try (OutputStream os = tmpLocation.getOutputStream()) {
      Files.copy(artifactFile.toPath(), os);
    }

    return tmpLocation;
  }
}
