/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.remote;

import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.test.ArtifactManager;

import java.util.Map;

/**
 * {@link ArtifactManager} for use in integration tests.
 */
public class RemoteArtifactManager implements ArtifactManager {
  private final ArtifactClient artifactClient;
  private final ArtifactId artifactId;

  public RemoteArtifactManager(ClientConfig clientConfig, RESTClient restClient, ArtifactId artifactId) {
    this.artifactClient = new ArtifactClient(clientConfig, restClient);
    this.artifactId = artifactId;
  }

  @Override
  public void writeProperties(Map<String, String> properties) throws Exception {
    artifactClient.writeProperties(artifactId.toId(), properties);
  }

  @Override
  public void removeProperties() throws Exception {
    artifactClient.deleteProperties(artifactId.toId());
  }

  @Override
  public void delete() throws Exception {
    artifactClient.delete(artifactId.toId());
  }
}
