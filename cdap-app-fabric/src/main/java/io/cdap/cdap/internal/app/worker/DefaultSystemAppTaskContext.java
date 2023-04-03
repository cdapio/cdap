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

package io.cdap.cdap.internal.app.worker;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.app.services.AbstractServiceDiscoverer;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.DefaultPluginConfigurer;
import io.cdap.cdap.internal.app.DefaultServicePluginConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for {@link io.cdap.cdap.api.service.worker.SystemAppTaskContext}
 */
public class DefaultSystemAppTaskContext extends AbstractServiceDiscoverer implements
    SystemAppTaskContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSystemAppTaskContext.class);

  private final PreferencesFetcher preferencesFetcher;
  private final CConfiguration cConf;
  private final PluginFinder pluginFinder;
  private final SecureStore secureStore;
  private final String serviceName;
  private final RetryStrategy retryStrategy;
  private final ArtifactManager artifactManager;
  private final PluginInstantiator instantiator;
  private final File pluginsDir;
  private final io.cdap.cdap.proto.id.ArtifactId protoArtifactId;
  private final RemoteClientFactory remoteClientFactory;
  private final ClassLoader artifactClassLoader;
  private final FeatureFlagsProvider featureFlagsProvider;

  DefaultSystemAppTaskContext(CConfiguration cConf, PreferencesFetcher preferencesFetcher,
      PluginFinder pluginFinder,
      SecureStore secureStore, String artifactNameSpace, ArtifactId artifactId,
      ClassLoader artifactClassLoader, ArtifactManagerFactory artifactManagerFactory,
      String serviceName, RemoteClientFactory remoteClientFactory) {
    super(artifactNameSpace);
    this.cConf = cConf;
    this.serviceName = serviceName;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, Constants.Retry.SERVICE_PREFIX);
    this.preferencesFetcher = preferencesFetcher;
    this.pluginFinder = pluginFinder;
    this.secureStore = secureStore;
    this.remoteClientFactory = remoteClientFactory;
    this.artifactClassLoader = artifactClassLoader;
    this.artifactManager = artifactManagerFactory.create(new NamespaceId(artifactNameSpace),
        retryStrategy);
    this.pluginsDir = createTempFolder();
    this.instantiator = new PluginInstantiator(cConf, artifactClassLoader, pluginsDir, true);
    this.protoArtifactId = Artifacts.toProtoArtifactId(new NamespaceId(artifactNameSpace),
        artifactId);
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
  }

  private File createTempFolder() {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    try {
      return Files.createTempDirectory(tmpDir.toPath(), "plugins").toFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return featureFlagsProvider.isFeatureEnabled(name);
  }

  @Override
  public Map<String, String> getPreferencesForNamespace(String namespace, boolean resolved)
      throws Exception {
    try {
      return Retries.callWithRetries(
          () -> preferencesFetcher.get(new NamespaceId(namespace), resolved).getProperties(),
          retryStrategy);
    } catch (NotFoundException nfe) {
      throw new IllegalArgumentException(String.format("Namespace '%s' does not exist", namespace),
          nfe);
    }
  }

  @Override
  public PluginConfigurer createPluginConfigurer(String namespace) {
    return new DefaultPluginConfigurer(protoArtifactId, new NamespaceId(namespace), instantiator,
        pluginFinder);
  }

  @Override
  public ServicePluginConfigurer createServicePluginConfigurer(String namespace) {
    return new DefaultServicePluginConfigurer(protoArtifactId, new NamespaceId(namespace),
        instantiator, pluginFinder,
        artifactClassLoader);
  }

  @Override
  public Map<String, String> evaluateMacros(String namespace, Map<String, String> macros,
      MacroEvaluator evaluator,
      MacroParserOptions options) throws InvalidMacroException {
    MacroParser macroParser = new MacroParser(evaluator, options);
    Map<String, String> evaluated = new HashMap<>();

    for (Map.Entry<String, String> property : macros.entrySet()) {
      String key = property.getKey();
      String val = property.getValue();
      evaluated.put(key, macroParser.parse(val));
    }
    return evaluated;
  }

  @Override
  public ArtifactManager getArtifactManager() {
    return artifactManager;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }

  @Override
  protected RemoteClientFactory getRemoteClientFactory() {
    return remoteClientFactory;
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    return Retries.callWithRetries(() -> secureStore.list(namespace), retryStrategy);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    return Retries.callWithRetries(() -> secureStore.get(namespace, name), retryStrategy);
  }

  @Override
  public SecureStoreMetadata getMetadata(String namespace, String name) throws Exception {
    return Retries.callWithRetries(() -> secureStore.getMetadata(namespace, name), retryStrategy);
  }

  @Override
  public byte[] getData(String namespace, String name) throws Exception {
    return Retries.callWithRetries(() -> secureStore.getData(namespace, name), retryStrategy);
  }

  @Override
  public void close() {
    try {
      try {
        instantiator.close();
      } finally {
        DirUtils.deleteDirectoryContents(pluginsDir, true);
      }
    } catch (IOException e) {
      LOG.warn("Error while cleaning up resources.", e);
    }
  }
}
