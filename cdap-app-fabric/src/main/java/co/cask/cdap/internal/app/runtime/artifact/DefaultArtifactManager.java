/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.artifact;

  import co.cask.cdap.api.artifact.ArtifactInfo;
  import co.cask.cdap.proto.id.NamespaceId;
  import co.cask.cdap.security.spi.authentication.AuthenticationContext;
  import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
  import com.google.common.annotations.VisibleForTesting;
  import com.google.inject.Inject;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;

  import java.util.ArrayList;
  import java.util.List;
  import javax.annotation.Nullable;

/**
 * Artifact store read only
 */
public class DefaultArtifactManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultArtifactManager.class);

  private final ArtifactStore artifactStore;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @VisibleForTesting
  @Inject
  public DefaultArtifactManager(ArtifactStore artifactStore, AuthorizationEnforcer authorizationEnforcer,
                                AuthenticationContext authenticationContext) {
    this.artifactStore = artifactStore;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  public List<ArtifactInfo> listArtifacts(NamespaceId namespaceId) {
    //todo implement
    return new ArrayList<>();
  }


  public ClassLoader createClassLoader(ArtifactInfo artifactInfo, @Nullable ClassLoader parentClassLoader) {
    // todo implement
    return this.getClass().getClassLoader();
  }
}
