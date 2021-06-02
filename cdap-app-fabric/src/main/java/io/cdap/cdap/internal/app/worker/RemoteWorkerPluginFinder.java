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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.Inject;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

public class RemoteWorkerPluginFinder extends RemotePluginFinder {
  private final ArtifactLocalizerClient artifactLocalizerClient;

  @Inject
  RemoteWorkerPluginFinder(CConfiguration cConf,
                           DiscoveryServiceClient discoveryServiceClient,
                           AuthenticationContext authenticationContext,
                           LocationFactory locationFactory,
                           ArtifactLocalizerClient artifactLocalizerClient) {
    super(cConf, discoveryServiceClient, authenticationContext, locationFactory);
    this.artifactLocalizerClient = artifactLocalizerClient;
  }

  @Override
  public Location getArtifactLocation(
    ArtifactId artifactId) throws IOException, ArtifactNotFoundException, UnauthorizedException {
    return artifactLocalizerClient.getArtifactLocation(artifactId);
  }
}
