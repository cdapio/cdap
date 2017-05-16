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


import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
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
  private final File tmpDir;
  private final ClassLoader bootstrapClassLoader;

  @VisibleForTesting
  @Inject
  public DefaultArtifactManager(CConfiguration cConf,
                                ArtifactStore artifactStore, AuthorizationEnforcer authorizationEnforcer,
                                AuthenticationContext authenticationContext) {
    this.artifactStore = artifactStore;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    // There is no reliable way to get bootstrap ClassLoader from Java (System.class.getClassLoader() may return null).
    // A URLClassLoader with no URLs and with a null parent will load class from bootstrap ClassLoader only.
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  public List<ArtifactInfo> listArtifacts(NamespaceId namespaceId) throws IOException {
    List<ArtifactDetail> artifactDetails = artifactStore.getArtifacts(namespaceId);
    return Lists.transform(artifactDetails, new Function<ArtifactDetail, ArtifactInfo>() {
      @Nullable
      @Override
      public ArtifactInfo apply(@Nullable ArtifactDetail input) {
        // transform artifactDetail to artifactInfo
        return new ArtifactInfo(input.getDescriptor().getArtifactId(),
                                input.getMeta().getClasses(), input.getMeta().getProperties());
      }
    });
  }

  /**
   * Ensures that the logged-in user has a {@link Action privilege} on the specified dataset instance.
   *
   * @param artifactId the {@link co.cask.cdap.proto.id.ArtifactId} to check for privileges
   * @throws UnauthorizedException if the logged in user has no {@link Action privileges} on the specified dataset
   */
  private void ensureAccess(co.cask.cdap.proto.id.ArtifactId artifactId) throws Exception {
    // No authorization for system artifacts
    if (NamespaceId.SYSTEM.equals(artifactId.getParent())) {
      return;
    }
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!filter.apply(artifactId)) {
      throw new UnauthorizedException(principal, artifactId);
    }
  }


  /**
   * Create a class loader with artifact jar unpacked contents and parent for this classloader is the supplied
   * parentClassLoader, if that parent classloader is null, bootstrap classloader is used as parent.
   * This is a closeabled classloader, caller should call close when he is done using it, during close directory
   * cleanup will be performed.
   * @param namespace
   * @param artifactInfo
   * @param parentClassLoader
   * @return CloseableClassLoader
   * @throws Exception if artifact is not found or there were any error while getting artifact or if its unauthorized
   */
  public CloseableClassLoader createClassLoader(
    NamespaceId namespace, ArtifactInfo artifactInfo, @Nullable ClassLoader parentClassLoader) throws Exception {

    ensureAccess(new ArtifactId(namespace.getNamespace(), artifactInfo.getName(), artifactInfo.getVersion()));
    ArtifactDetail artifactDetail = artifactStore.getArtifact(
      new co.cask.cdap.proto.id.ArtifactId(namespace.getNamespace(),
                                           artifactInfo.getName(), artifactInfo.getVersion()).toId());

    final File unpackedDir = DirUtils.createTempDir(tmpDir);
    BundleJarUtil.unJar(artifactDetail.getDescriptor().getLocation(), unpackedDir);

    return new CloseableClassLoader(
      new DirectoryClassLoader(unpackedDir, parentClassLoader == null ? bootstrapClassLoader : parentClassLoader),
      new DeleteContents(unpackedDir));
  }

  class DeleteContents implements Closeable {
    private final File directory;

    DeleteContents(File directory) {
      this.directory = directory;
    }
    @Override
    public void close() throws IOException {
      DirUtils.deleteDirectoryContents(directory);
    }
  }
}
