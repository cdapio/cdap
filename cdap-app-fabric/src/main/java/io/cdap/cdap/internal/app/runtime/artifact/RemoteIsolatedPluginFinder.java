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
 *
 */

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.HttpCodes;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizer;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import javax.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Plugin finder that is used to find plugin when program is run in ISOLATED mode, the only
 * difference from {@link RemotePluginFinder} is that it will fetch the artifact onto local file
 * system.
 *
 * This class does not use {@link ArtifactLocalizer} because each program run is isolated and the
 * plugin finder is one time and happens in a single JVM process
 */
public class RemoteIsolatedPluginFinder extends RemotePluginFinder {

  public static final String ISOLATED_PLUGIN_DIR = "IsolatedPluginDir";
  private final File pluginDir;

  @Inject
  public RemoteIsolatedPluginFinder(LocationFactory locationFactory,
      RemoteClientFactory remoteClientFactory,
      @Named(ISOLATED_PLUGIN_DIR) String pluginDir) {
    super(locationFactory, remoteClientFactory);
    this.pluginDir = new File(pluginDir);
  }

  public RemoteIsolatedPluginFinder(LocationFactory locationFactory, RemoteClient remoteClient,
      RemoteClient remoteClientInternal, @Named(ISOLATED_PLUGIN_DIR) String pluginDir) {
    super(locationFactory, remoteClient, remoteClientInternal);
    this.pluginDir = new File(pluginDir);
  }

  @Override
  protected Location getArtifactLocation(ArtifactId artifactId)
      throws IOException, ArtifactNotFoundException, UnauthorizedException {
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
        artifactId.getNamespace(),
        artifactId.getArtifact(),
        artifactId.getVersion(),
        artifactId.getNamespace().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())
            ? ArtifactScope.SYSTEM.name().toLowerCase() : ArtifactScope.USER.name().toLowerCase());

    HttpURLConnection urlConn = remoteClientInternal.openConnection(HttpMethod.GET, url);

    try {
      int responseCode = urlConn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        if (HttpCodes.isRetryable(responseCode)) {
          throw new ServiceUnavailableException(
              Constants.Service.APP_FABRIC_HTTP, Constants.Service.APP_FABRIC_HTTP
              + " service is not available with status " + responseCode);
        }
        if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new ArtifactNotFoundException(artifactId);
        }
        throw new IOException(
            String.format("Exception while downloading artifact for artifact %s with reason: %s",
                artifactId, urlConn.getResponseMessage()));
      }

      File artifactLocation = new File(pluginDir,
          Artifacts.getFileName(artifactId.toApiArtifactId()));

      try (InputStream in = urlConn.getInputStream()) {
        Files.copy(in, artifactLocation.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }

      return Locations.toLocation(artifactLocation);
    } finally {
      urlConn.disconnect();
    }
  }
}
