/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Provides various REST endpoints to execute user code via {@link DatasetAdminOpHTTPHandler}.
 */
public class DatasetOpExecutorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetOpExecutorService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public DatasetOpExecutorService(CConfiguration cConf, SConfiguration sConf, DiscoveryService discoveryService,
                                  CommonNettyHttpServiceFactory commonNettyHttpServiceFactory,
                                  @Named(Constants.Service.DATASET_EXECUTOR) Set<HttpHandler> handlers) {

    this.discoveryService = discoveryService;

    int workerThreads = cConf.getInt(Constants.Dataset.Executor.WORKER_THREADS, 10);
    int execThreads = cConf.getInt(Constants.Dataset.Executor.EXEC_THREADS, 30);

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(Constants.Service.DATASET_EXECUTOR)
      .setHttpHandlers(handlers)
      .setHost(cConf.get(Constants.Dataset.Executor.ADDRESS))
      .setPort(cConf.getInt(Constants.Dataset.Executor.PORT))
      .setWorkerThreadPoolSize(workerThreads)
      .setExecThreadPoolSize(execThreads)
      .setConnectionBacklog(20000);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getEntityName(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.DATASET_EXECUTOR));
    LOG.info("Starting DatasetOpExecutorService...");

    httpService.start();
    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.DATASET_EXECUTOR, httpService)));
    LOG.info("DatasetOpExecutorService started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping DatasetOpExecutorService...");

    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      httpService.stop();
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }

  public NettyHttpService getHttpService() {
    return httpService;
  }
}
