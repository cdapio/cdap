/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.metadata.MetadataReader;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.SystemHttpServiceContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.internal.app.DefaultPluginConfigurer;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.services.DefaultSystemTableConfigurer;
import co.cask.cdap.master.spi.program.Program;
import co.cask.cdap.master.spi.program.ProgramOptions;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Default implementation of HttpServiceContext which simply stores and retrieves the
 * spec provided when this class is instantiated
 */
public class BasicHttpServiceContext extends AbstractContext implements SystemHttpServiceContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicHttpServiceContext.class);

  private final CConfiguration cConf;
  private final NamespaceId namespaceId;
  private final ArtifactId artifactId;
  private final HttpServiceHandlerSpecification spec;
  private final int instanceId;
  private final AtomicInteger instanceCount;
  private final ArtifactManager artifactManager;
  private final PluginFinder pluginFinder;
  private final TransactionRunner transactionRunner;
  private final Collection<Closeable> closeables;

  /**
   * Creates a BasicHttpServiceContext for the given HttpServiceHandlerSpecification.
   * @param program program of the context.
   * @param programOptions program options for the program execution context
   * @param spec spec of the service handler of this context. If {@code null} is provided, this context
   *             is not associated with any service handler (e.g. for the http server itself).
   * @param instanceId instanceId of the component.
   * @param instanceCount total number of instances of the component.
   * @param metricsCollectionService metricsCollectionService to use for emitting metrics.
   * @param dsFramework dsFramework to use for getting datasets.
   * @param discoveryServiceClient discoveryServiceClient used to do service discovery.
   * @param txClient txClient to do transaction operations.
   * @param pluginInstantiator {@link PluginInstantiator}
   * @param secureStore The {@link SecureStore} for this context
   */
  public BasicHttpServiceContext(Program program, ProgramOptions programOptions, CConfiguration cConf,
                                 @Nullable HttpServiceHandlerSpecification spec,
                                 int instanceId, AtomicInteger instanceCount,
                                 MetricsCollectionService metricsCollectionService,
                                 DatasetFramework dsFramework, DiscoveryServiceClient discoveryServiceClient,
                                 TransactionSystemClient txClient, @Nullable PluginInstantiator pluginInstantiator,
                                 SecureStore secureStore, SecureStoreManager secureStoreManager,
                                 MessagingService messagingService,
                                 ArtifactManager artifactManager, MetadataReader metadataReader,
                                 MetadataPublisher metadataPublisher,
                                 NamespaceQueryAdmin namespaceQueryAdmin,
                                 PluginFinder pluginFinder, TransactionRunner transactionRunner) {
    super(program, programOptions, cConf, spec == null ? Collections.emptySet() : spec.getDatasets(),
          dsFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, createMetricsTags(spec, instanceId),
          secureStore, secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin);
    this.cConf = cConf;
    this.namespaceId = program.getId().getNamespaceId();
    this.artifactId = ProgramRunners.getArtifactId(programOptions);
    this.spec = spec;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.artifactManager = artifactManager;
    this.pluginFinder = pluginFinder;
    this.transactionRunner = transactionRunner;
    this.closeables = new ArrayList<>();
  }

  public static Map<String, String> createMetricsTags(@Nullable HttpServiceHandlerSpecification spec, int instanceId) {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId));
    if (spec != null) {
      tags.put(Constants.Metrics.Tag.HANDLER, spec.getName());
    }
    return tags;
  }

  /**
   * @return the {@link HttpServiceHandlerSpecification} for this context or {@code null} if there is no service
   *         handler associated with this context.
   */
  @Nullable
  @Override
  public HttpServiceHandlerSpecification getSpecification() {
    return spec;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount.get();
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public PluginConfigurer createPluginConfigurer() {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    try {
      File pluginsDir = Files.createTempDirectory(tmpDir.toPath(), "plugins").toFile();
      PluginInstantiator instantiator = new PluginInstantiator(cConf, getProgram().getClassLoader(), pluginsDir);
      closeables.add(() -> {
        try {
          instantiator.close();
        } finally {
          DirUtils.deleteDirectoryContents(pluginsDir, true);
        }
      });
      return new DefaultPluginConfigurer(artifactId, namespaceId, instantiator, pluginFinder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ArtifactInfo> listArtifacts() throws IOException {
    return artifactManager.listArtifacts();
  }

  @Override
  public List<ArtifactInfo> listArtifacts(String namespace) throws IOException {
    return artifactManager.listArtifacts(namespace);
  }

  @Override
  public CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
                                                @Nullable ClassLoader parentClassLoader) throws IOException {
    return artifactManager.createClassLoader(artifactInfo, parentClassLoader);
  }

  @Override
  public CloseableClassLoader createClassLoader(String namespace, ArtifactInfo artifactInfo,
                                                @Nullable ClassLoader parentClassLoader) throws IOException {
    return artifactManager.createClassLoader(namespace, artifactInfo, parentClassLoader);
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      // should not happen in normal circumstances, as this is checked when the application is deployed.
      // could possibly be called if the user is directly casting to a SystemHttpServiceContext in user services.
      throw new UnauthorizedException("System table transactions can only be run by "
                                        + "applications in the system namespace.");
    }
    // table names are prefixed to prevent clashes with CDAP platform tables.
    transactionRunner.run(context -> runnable.run(
      tableId -> context.getTable(new StructuredTableId(DefaultSystemTableConfigurer.PREFIX + tableId.getName()))));
  }

  /**
   * Releases resources that were created for an endpoint call but are no longer needed for future calls.
   */
  public void releaseCallResources() {
    for (Closeable closeable : closeables) {
      try {
        closeable.close();
      } catch (IOException e) {
        LOG.warn("Error while cleaning up service resources.", e);
      }
    }
    closeables.clear();
  }
}
