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

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.service.worker.RunnableTaskSystemAppContext;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.DefaultPluginConfigurer;
import io.cdap.cdap.internal.app.runtime.DefaultAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.BasicMessagingAdmin;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link RunnableTaskSystemAppContext}
 */
public class DefaultRunnableTaskSystemAppContext implements RunnableTaskSystemAppContext, ServiceDiscoverer,
  SecureStore {

  private final Admin admin;
  private final PreferencesFetcher preferencesFetcher;
  private final CConfiguration cConf;
  private final ClassLoader classLoader;
  private final PluginFinder pluginFinder;
  private final ArtifactId artifactId;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final SecureStore secureStore;
  private final String namespace;
  private final RetryStrategy retryStrategy;

  DefaultRunnableTaskSystemAppContext(CConfiguration cConf, DatasetFramework dsFramework, String namespace,
                                      SecureStoreManager secureStoreManager, MessagingService messagingService,
                                      RetryStrategy retryStrategy, NamespaceQueryAdmin namespaceQueryAdmin,
                                      PreferencesFetcher preferencesFetcher, ClassLoader classLoader,
                                      PluginFinder pluginFinder, ArtifactId artifactId,
                                      DiscoveryServiceClient discoveryServiceClient, SecureStore secureStore) {
    this.cConf = cConf;
    this.namespace = namespace;
    NamespaceId namespaceId = new NamespaceId(namespace);
    this.admin = new DefaultAdmin(dsFramework, namespaceId, secureStoreManager,
                                  new BasicMessagingAdmin(messagingService, namespaceId),
                                  retryStrategy, null, namespaceQueryAdmin);
    this.retryStrategy = retryStrategy;
    this.preferencesFetcher = preferencesFetcher;
    this.classLoader = classLoader;
    this.pluginFinder = pluginFinder;
    this.artifactId = artifactId;
    this.discoveryServiceClient = discoveryServiceClient;
    this.secureStore = secureStore;
  }

  @Override
  public Admin getAdmin() {
    return admin;
  }

  @Override
  public Map<String, String> getPreferencesForNamespace(String namespace, boolean resolved) throws IOException {
    try {
      return preferencesFetcher.get(new NamespaceId(namespace), resolved).getProperties();
    } catch (NotFoundException nfe) {
      throw new IllegalArgumentException(String.format("Namespace '%s' does not exist", namespace), nfe);
    }
  }

  @Override
  public PluginConfigurer createPluginConfigurer(String namespace) throws IOException {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File pluginsDir = Files.createTempDirectory(tmpDir.toPath(), "plugins").toFile();
    PluginInstantiator instantiator = new PluginInstantiator(cConf, classLoader, pluginsDir);
    io.cdap.cdap.proto.id.ArtifactId protoArtifactId =
      new io.cdap.cdap.proto.id.ArtifactId(namespace, artifactId.getName(), artifactId.getVersion().getVersion());
    return new DefaultPluginConfigurer(protoArtifactId, new NamespaceId(namespace), instantiator, pluginFinder);
  }

  @Override
  public Map<String, String> evaluateMacros(String namespace, Map<String, String> macros, MacroEvaluator evaluator,
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
  public ServiceDiscoverer getServiceDiscoverer() {
    return this;
  }

  @Override
  public SecureStore getSecureStore() {
    return null;
  }

  @Override
  public URL getServiceURL(String namespaceId, String applicationId, String serviceId) {
    try {
      return createRemoteClient(namespaceId, applicationId, serviceId).resolve("");
    } catch (ServiceUnavailableException e) {
      return null;
    }
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return getServiceURL(namespace, applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    throw new UnsupportedOperationException("Application Id is required.");
  }

  @Nullable
  @Override
  public HttpURLConnection openConnection(String namespaceId, String applicationId,
                                          String serviceId, String methodPath) throws IOException {
    try {
      return createRemoteClient(namespaceId, applicationId, serviceId).openConnection(methodPath);
    } catch (ServiceUnavailableException e) {
      return null;
    }
  }

  private RemoteClient createRemoteClient(String namespaceId, String applicationId, String serviceId) {
    String discoveryName = ServiceDiscoverable.getName(namespaceId, applicationId, ProgramType.SERVICE, serviceId);
    String basePath = String.format("%s/namespaces/%s/apps/%s/services/%s/methods/",
                                    Constants.Gateway.API_VERSION_3_TOKEN, namespaceId, applicationId, serviceId);
    return new RemoteClient(discoveryServiceClient, discoveryName, HttpRequestConfig.DEFAULT, basePath);
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    return Retries.callWithRetries(() -> secureStore.list(namespace), retryStrategy);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    return Retries.callWithRetries(() -> secureStore.get(namespace, name), retryStrategy);
  }
}
